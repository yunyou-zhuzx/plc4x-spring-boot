package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.exceptions.PlcConnectionException;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 为驱动的阻塞建连和关闭提供等待上限；超时后保留实际任务，禁止重叠操作。
 */
final class PlcLifecycleExecutor {

	/**
	 * 所有设备共享有界执行器，异常驱动不能通过持续超时无限创建后台线程。
	 */
	private static final AtomicInteger THREAD_NUMBER = new AtomicInteger();
	private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(
			8, 8, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(64), runnable -> {
		final Thread thread = new Thread(runnable, "plc-lifecycle-" + THREAD_NUMBER.incrementAndGet());
		thread.setDaemon(true);
		return thread;
	}, new ThreadPoolExecutor.AbortPolicy());

	static {
		EXECUTOR.allowCoreThreadTimeOut(true);
	}

	/**
	 * 即使调用方已超时，任务真正退出前也不允许下一次建连或关闭。
	 */
	private volatile Operation active;

	/**
	 * 创建连接本身也可能阻塞，尚未返回连接对象时同样必须保留客户端所有权。
	 */
	boolean hasPendingOperation() {
		return Objects.nonNull(active) && !active.finished.isDone();
	}

	boolean isCancellationRequested() {
		return Objects.nonNull(active) && active.cancelled;
	}

	/**
	 * 执行一个生命周期操作，调用者须使用客户端生命周期锁串行访问。
	 */
	void run(final CheckedOperation operation, final long timeoutMillis) throws Exception {
		if (Objects.nonNull(active) && !active.finished.isDone()) {
			throw new PlcConnectionException("Previous PLC lifecycle operation is still running; retain client and retry later");
		}
		final Operation submitted = new Operation(operation);
		active = submitted;
		try {
			EXECUTOR.execute(submitted);
		} catch (RejectedExecutionException failure) {
			submitted.finished.completeExceptionally(failure);
			throw new PlcConnectionException("PLC lifecycle executor is busy; retry later", failure);
		}
		try {
			submitted.finished.get(timeoutMillis, TimeUnit.MILLISECONDS);
		} catch (TimeoutException failure) {
			submitted.cancel();
			throw new PlcConnectionException("PLC lifecycle operation timed out after " + timeoutMillis + " ms", failure);
		} catch (InterruptedException failure) {
			submitted.cancel();
			Thread.currentThread().interrupt();
			throw failure;
		} catch (ExecutionException failure) {
			final Throwable cause = failure.getCause();
			if (cause instanceof Exception exception) {
				throw exception;
			}
			if (cause instanceof Error error) {
				throw error;
			}
			throw new PlcConnectionException("PLC lifecycle operation failed", cause);
		}
	}

	/**
	 * 对接驱动声明的检查异常。
	 */
	@FunctionalInterface
	interface CheckedOperation {
		void run() throws Exception;
	}

	/**
	 * 使用实际执行完成状态，避免 Future.cancel() 把仍在运行的任务误报为已结束。
	 */
	private static final class Operation implements Runnable {

		private final CheckedOperation operation;
		private final CompletableFuture<Void> finished = new CompletableFuture<>();
		private volatile Thread worker;
		private volatile boolean cancelled;
		private final ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();

		private Operation(final CheckedOperation operation) {
			this.operation = operation;
		}

		@Override
		public void run() {
			worker = Thread.currentThread();
			final ClassLoader previousLoader = worker.getContextClassLoader();
			try {
				worker.setContextClassLoader(contextLoader);
				if (cancelled) {
					throw new InterruptedException("PLC lifecycle operation cancelled before start");
				}
				operation.run();
				finished.complete(null);
			} catch (Throwable failure) {
				// 执行器边界必须转交包括 Error 在内的失败，不能让等待方一直等待。
				finished.completeExceptionally(failure);
			} finally {
				Thread.currentThread().setContextClassLoader(previousLoader);
				synchronized (this) {
					worker = null;
				}
			}
		}

		/**
		 * 移除未执行的任务；正在执行时只发送中断，保留其真实完成状态。
		 */
		private synchronized void cancel() {
			if (finished.isDone()) {
				return;
			}
			cancelled = true;
			if (EXECUTOR.remove(this)) {
				finished.completeExceptionally(new InterruptedException("PLC lifecycle operation cancelled in queue"));
			} else {
				final Thread running = worker;
				if (Objects.nonNull(running)) {
					running.interrupt();
				}
			}
		}
	}
}
