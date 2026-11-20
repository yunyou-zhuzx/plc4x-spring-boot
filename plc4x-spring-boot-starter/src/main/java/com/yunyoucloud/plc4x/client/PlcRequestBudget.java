package com.yunyoucloud.plc4x.client;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 将外层请求的剩余预算传递给同步创建的协议事务，避免排队时间重新计时。
 */
final class PlcRequestBudget {

	private static final ThreadLocal<Budget> DEADLINE = new ThreadLocal<>();

	private PlcRequestBudget() {
	}

	static <T> CompletableFuture<T> submit(final long deadline, final Supplier<CompletableFuture<T>> operation) {
		final Budget previous = DEADLINE.get();
		final Budget budget = new Budget(deadline);
		DEADLINE.set(budget);
		try {
			final CompletableFuture<T> result = operation.get();
			result.whenComplete((value, failure) -> {
				if (Objects.nonNull(failure)) {
					budget.cancel();
				}
			});
			return result;
		} catch (RuntimeException | Error failure) {
			budget.cancel();
			throw failure;
		} finally {
			if (Objects.isNull(previous)) {
				DEADLINE.remove();
			} else {
				DEADLINE.set(previous);
			}
		}
	}

	static long remainingMillis(final long fallback) {
		final Budget deadline = DEADLINE.get();
		if (Objects.isNull(deadline)) {
			return fallback;
		}
		if (deadline.cancelled || Thread.currentThread().isInterrupted()) {
			return 0;
		}
		final long remaining = deadline.deadline - System.nanoTime();
		return remaining <= 0 ? 0 : Math.min(fallback, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
	}

	static void attach(final CompletableFuture<?> response) {
		final Budget budget = DEADLINE.get();
		if (Objects.nonNull(budget)) {
			budget.children.add(response);
			response.whenComplete((value, failure) -> budget.children.remove(response));
			if (budget.cancelled) {
				response.cancel(false);
			}
		}
	}

	private static final class Budget {
		private final long deadline;
		private final java.util.Set<CompletableFuture<?>> children = java.util.concurrent.ConcurrentHashMap.newKeySet();
		private volatile boolean cancelled;

		private Budget(final long deadline) {
			this.deadline = deadline;
		}

		private void cancel() {
			cancelled = true;
			children.forEach(response -> response.cancel(false));
		}
	}
}
