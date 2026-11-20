package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager;
import cn.hutool.core.collection.CollUtil;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 有界事务队列，成功、错误、取消、排队超时及关闭都释放同一个事务槽。
 */
final class ManagedRequestTransactionManager extends RequestTransactionManager {

	/**
	 * 限制每个连接保存的事务，避免设备无响应时持续积压。
	 */
	private static final int MAX_PENDING = 64;
	private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();
	private static final ThreadLocal<Transaction> CURRENT = new ThreadLocal<>();

	/**
	 * 报文注册完成后绑定真实等待器；正常响应会主动解除绑定。
	 */
	static Runnable onCurrentCompletion(final Runnable cleanup) {
		final Transaction transaction = CURRENT.get();
		return Objects.isNull(transaction) ? () -> {
		} : transaction.bindCleanup(cleanup);
	}

	/**
	 * 队列、运行集合和关闭状态使用同一把锁。
	 */
	private final Object lock = new Object();
	private final Deque<Transaction> queued = new ArrayDeque<>();
	private final Set<Transaction> running = new HashSet<>();
	private final AtomicInteger sequence = new AtomicInteger();
	private final ThreadLocal<RequestScope> scope = new ThreadLocal<>();
	private final Set<CompletableFuture<?>> responses = ConcurrentHashMap.newKeySet();
	private final AtomicInteger responseCount = new AtomicInteger();
	private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS,
			new ArrayBlockingQueue<>(MAX_PENDING), runnable -> {
		final Thread thread = new Thread(runnable, "plc-request-" + THREAD_NUMBER.incrementAndGet());
		thread.setDaemon(true);
		return thread;
	}, new ThreadPoolExecutor.AbortPolicy());
	private int concurrency = 1;
	private boolean closed;
	private int suspensionDepth;

	ManagedRequestTransactionManager() {
		worker.allowCoreThreadTimeOut(true);
	}

	/**
	 * 为上游返回 void 的原生发布操作提供可取消的结果，终态由其实际事务决定。
	 */
	CompletableFuture<Void> trackOperation(final Runnable operation, final long timeoutMillis) {
		return track(() -> {
			final RequestScope current = scope.get();
			operation.run();
			return CompletableFuture.allOf(current.transactions().stream()
					.map(transaction -> transaction.outcome).toArray(CompletableFuture[]::new));
		}, timeoutMillis);
	}

	/**
	 * 将驱动实际创建的事务绑定到协议 Future，超时预算也包含排队时间。
	 */
	<T> CompletableFuture<T> track(final Supplier<CompletableFuture<T>> request, final long timeoutMillis) {
		final long budget = PlcRequestBudget.remainingMillis(timeoutMillis);
		if (budget <= 0) {
			return CompletableFuture.failedFuture(new TimeoutException("PLC request budget exhausted before submission"));
		}
		if (responseCount.incrementAndGet() > MAX_PENDING) {
			responseCount.decrementAndGet();
			return CompletableFuture.failedFuture(new RejectedExecutionException("Too many pending PLC responses"));
		}
		final RequestScope current = new RequestScope(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budget));
		final Set<Transaction> transactions = current.transactions;
		final RequestScope previous = scope.get();
		scope.set(current);
		try {
			final CompletableFuture<T> result = Objects.requireNonNull(request.get(), "Protocol future");
			final boolean alreadyClosed;
			synchronized (lock) {
				responses.add(result);
				alreadyClosed = closed;
			}
			current.failure.whenComplete((ignored, failure) -> {
				if (Objects.nonNull(failure)) {
					result.completeExceptionally(failure);
				}
			});
			final CompletableFuture<T> tracked = new CompletableFuture<>();
			result.whenComplete((value, failure) -> {
				current.done = true;
				responses.remove(result);
				responseCount.decrementAndGet();
				for (final Transaction transaction : transactions) {
					if (Objects.isNull(failure)) {
						transaction.endRequest();
					} else {
						transaction.failRequest(failure);
					}
				}
				if (Objects.isNull(failure)) {
					tracked.complete(value);
				} else {
					tracked.completeExceptionally(failure);
				}
			});
			tracked.whenComplete((value, failure) -> {
				if (tracked.isCancelled()) {
					result.cancel(false);
				}
			});
			if (alreadyClosed) {
				result.completeExceptionally(new PlcConnectionException("PLC protocol has been closed"));
			}
			result.orTimeout(budget, TimeUnit.MILLISECONDS);
			PlcRequestBudget.attach(tracked);
			return tracked;
		} catch (RuntimeException | Error failure) {
			current.done = true;
			responseCount.decrementAndGet();
			transactions.forEach(transaction -> transaction.failRequest(failure));
			return CompletableFuture.failedFuture(failure);
		} finally {
			if (Objects.isNull(previous)) {
				scope.remove();
			} else {
				scope.set(previous);
			}
		}
	}

	@Override
	public RequestTransaction startRequest() {
		final RequestScope current = scope.get();
		final Transaction transaction = new Transaction(sequence.getAndIncrement(), Objects.isNull(current) ? 0 : current.deadline());
		transaction.owner = current;
		if (Objects.nonNull(current)) {
			current.transactions.add(transaction);
			transaction.outcome.whenComplete((ignored, failure) -> {
				if (current.done) {
					current.transactions.remove(transaction);
				}
				if (Objects.nonNull(failure)) {
					current.failure.completeExceptionally(failure);
				}
			});
			if (current.done) {
				transaction.failRequest(new java.util.concurrent.CancellationException("Parent PLC request already completed"));
			}
		}
		return transaction;
	}

	@Override
	public int getNumberOfConcurrentRequests() {
		synchronized (lock) {
			return concurrency;
		}
	}

	@Override
	public void setNumberOfConcurrentRequests(final int count) {
		if (count < 1 || count > MAX_PENDING) {
			throw new IllegalArgumentException("PLC transaction concurrency must be between 1 and 64");
		}
		synchronized (lock) {
			concurrency = count;
			dispatch();
		}
	}

	@Override
	public int getNumberOfActiveRequests() {
		synchronized (lock) {
			return running.size();
		}
	}

	/**
	 * 返回尚未结束的事务总数，供资源回收回归检查使用。
	 */
	int pendingCount() {
		synchronized (lock) {
			return running.size() + queued.size();
		}
	}

	int pendingResponseCount() {
		return responseCount.get();
	}

	@Override
	public void shutdown() {
		synchronized (lock) {
			closed = true;
		}
		cancelPending(new PlcConnectionException("PLC protocol has been closed"));
		responses.forEach(response -> response.completeExceptionally(new PlcConnectionException("PLC protocol has been closed")));
		worker.shutdownNow();
		super.shutdown();
	}

	/**
	 * 先中止旧请求，再允许关闭会话等善后请求进入队列。
	 */
	void cancelPending(final Throwable failure) {
		final List<Transaction> pending;
		synchronized (lock) {
			suspensionDepth++;
			pending = new ArrayList<>(running);
			pending.addAll(queued);
		}
		try {
			pending.forEach(transaction -> transaction.failRequest(failure));
		} finally {
			synchronized (lock) {
				suspensionDepth--;
				dispatch();
			}
		}
	}

	void failPendingResponses(final Throwable failure) {
		cancelPending(failure);
		responses.forEach(response -> response.completeExceptionally(failure));
	}

	/**
	 * 调用时持有队列锁，先公布运行状态和 Future，再安排任务，消除失败回调竞态。
	 */
	private void dispatch() {
		while (!closed && suspensionDepth == 0 && running.size() < concurrency && CollUtil.isNotEmpty(queued)) {
			final Transaction transaction = queued.removeFirst();
			running.add(transaction);
			try {
				worker.execute((Runnable) transaction.completion);
			} catch (RejectedExecutionException failure) {
				transaction.failRequest(failure);
			}
		}
	}

	/**
	 * 事务对象自行持有终态，晚到的响应不能重复释放或误释放其他事务。
	 */
	private final class Transaction extends RequestTransaction {

		private final CompletableFuture<Void> outcome = new CompletableFuture<>();
		private Runnable operation;
		private Future<?> completion;
		private volatile boolean finished;
		private final long deadline;
		private RequestScope owner;
		private final Set<Runnable> cleanups = new HashSet<>();

		private ManagedRequestTransactionManager manager() {
			return ManagedRequestTransactionManager.this;
		}

		private Runnable bindCleanup(final Runnable cleanup) {
			final boolean completed;
			synchronized (lock) {
				completed = finished;
				if (!completed) {
					cleanups.add(cleanup);
				}
			}
			if (completed) {
				cleanup.run();
			}
			return () -> {
				synchronized (lock) {
					cleanups.remove(cleanup);
				}
			};
		}

		private Transaction(final int id, final long deadline) {
			super(ManagedRequestTransactionManager.this, id);
			this.deadline = deadline;
		}

		@Override
		public void submit(final Runnable task) {
			synchronized (lock) {
				if (closed || finished || Objects.nonNull(owner) && owner.done || pendingCount() >= MAX_PENDING || Objects.nonNull(completion)) {
					final RejectedExecutionException failure = new RejectedExecutionException("PLC transaction queue is closed or full");
					failRequest(failure);
					throw failure;
				}
				operation = task;
				completion = new FutureTask<>(() -> {
					if (!finished) {
						try {
							if (Objects.nonNull(owner) && owner.done || deadline != 0 && System.nanoTime() - deadline >= 0) {
								failRequest(new TimeoutException("PLC request expired while queued"));
							} else {
								CURRENT.set(this);
								if (Objects.nonNull(owner)) {
									scope.set(owner);
								}
								try {
									task.run();
								} finally {
									CURRENT.remove();
									scope.remove();
								}
							}
						} catch (RuntimeException | Error failure) {
							failRequest(failure);
						}
					}
				}, null);
				queued.addLast(this);
				dispatch();
			}
		}

		@Override
		public void endRequest() {
			finish(null);
		}

		@Override
		public void failRequest(final Throwable failure) {
			finish(Objects.requireNonNull(failure));
		}

		@Override
		public Future<?> getCompletionFuture() {
			return completion;
		}

		@Override
		public void setCompletionFuture(final Future<?> future) {
			completion = future;
		}

		@Override
		public void setOperation(final Runnable task) {
			operation = task;
		}

		/**
		 * 先从队列移除，再通知调用方，取消排队任务不会在以后写入设备。
		 */
		private void finish(final Throwable failure) {
			final List<Runnable> cleanup;
			synchronized (lock) {
				if (finished) {
					return;
				}
				finished = true;
				queued.remove(this);
				running.remove(this);
				if (Objects.nonNull(completion) && !completion.isDone()) {
					completion.cancel(false);
					worker.remove((Runnable) completion);
				}
				operation = null;
				cleanup = new ArrayList<>(cleanups);
				cleanups.clear();
				dispatch();
			}
			cleanup.forEach(Runnable::run);
			// CompletableFuture 可能同步执行业务回调，不能在持有队列锁时完成。
			if (Objects.isNull(failure)) {
				outcome.complete(null);
			} else {
				outcome.completeExceptionally(failure);
			}
		}
	}

	/**
	 * 一次协议调用创建的所有事务共享同一个截止时间。
	 */
	private static final class RequestScope {
		private final Set<Transaction> transactions = ConcurrentHashMap.newKeySet();
		private final CompletableFuture<Void> failure = new CompletableFuture<>();
		private final long deadline;
		private volatile boolean done;

		private RequestScope(final long deadline) {
			this.deadline = deadline;
		}

		private long deadline() {
			return deadline;
		}

		private Set<Transaction> transactions() {
			return transactions;
		}
	}

	/**
	 * 响应回调恢复所属请求上下文，异步阶段创建的事务继承取消状态和总截止时间。
	 */
	static java.util.function.Consumer<Object> bindCurrentCallback(final java.util.function.Consumer<Object> callback) {
		final Transaction transaction = CURRENT.get();
		final RequestScope owner = Objects.isNull(transaction) ? null : transaction.owner;
		if (Objects.isNull(owner)) {
			return callback;
		}
		return value -> {
			final ManagedRequestTransactionManager manager = transaction.manager();
			final RequestScope previous = manager.scope.get();
			manager.scope.set(owner);
			try {
				callback.accept(value);
			} finally {
				if (Objects.isNull(previous)) {
					manager.scope.remove();
				} else {
					manager.scope.set(previous);
				}
			}
		};
	}
}
