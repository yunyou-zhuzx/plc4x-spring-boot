package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.s7.readwrite.optimizer.S7Optimizer;
import org.apache.plc4x.java.spi.Plc4xProtocolBase;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * 保留 S7 拆包与合并算法，聚合请求取消时回收全部子请求，中断和过期后停止继续发送。
 */
class ManagedS7Optimizer extends S7Optimizer {
	@Override
	public CompletableFuture<PlcReadResponse> optimizedRead(final PlcReadRequest request, final Plc4xProtocolBase<?> protocol) {
		return send(processReadRequest(request, protocol.getDriverContext()), protocol::read,
				replies -> processReadResponses(request, replies, protocol.getDriverContext()));
	}

	@Override
	public CompletableFuture<PlcWriteResponse> optimizedWrite(final PlcWriteRequest request, final Plc4xProtocolBase<?> protocol) {
		return send(processWriteRequest(request, protocol.getDriverContext()), protocol::write,
				replies -> processWriteResponses(request, replies, protocol.getDriverContext()));
	}

	private <Q extends PlcRequest, R extends PlcResponse> CompletableFuture<R> send(final List<Q> requests,
																					final Function<Q, CompletableFuture<R>> sender, final Function<Map<Q, SubResponse<R>>, R> combine) {
		final Map<Q, CompletableFuture<R>> children = new LinkedHashMap<>();
		final CompletableFuture<R> parent = new CompletableFuture<>();
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PlcRequestBudget.remainingMillis(10000));
		parent.whenComplete((value, failure) -> {
			if (Objects.nonNull(failure)) {
				children.values().forEach(child -> child.cancel(false));
			}
		});
		try {
			for (final Q request : requests) {
				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException("S7 split request interrupted");
				}
				if (System.nanoTime() - deadline >= 0) {
					throw new TimeoutException("S7 split request budget exhausted");
				}
				final CompletableFuture<R> child = sender.apply(request);
				children.put(request, child);
				if (child.isCompletedExceptionally()) {
					child.join();
				}
				if (getMillisDelay() > 0 || getNanosDelay() > 0) {
					Thread.sleep(getMillisDelay(), getNanosDelay());
				}
			}
			CompletableFuture.allOf(children.values().toArray(CompletableFuture[]::new)).whenComplete((ignored, failure) -> {
				if (parent.isDone()) {
					return;
				}
				if (Objects.nonNull(failure)) {
					parent.completeExceptionally(failure);
					return;
				}
				try {
					final Map<Q, SubResponse<R>> replies = new LinkedHashMap<>();
					children.forEach((request, child) -> replies.put(request, new SubResponse<>(child.join())));
					parent.complete(combine.apply(replies));
				} catch (RuntimeException error) {
					parent.completeExceptionally(error);
				}
			});
		} catch (InterruptedException failure) {
			Thread.currentThread().interrupt();
			parent.completeExceptionally(failure);
		} catch (TimeoutException | RuntimeException failure) {
			parent.completeExceptionally(failure);
		}
		return parent.orTimeout(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())), TimeUnit.MILLISECONDS);
	}
}
