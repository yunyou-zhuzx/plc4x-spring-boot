package com.yunyoucloud.plc4x.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager.RequestTransaction;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class AsyncRequestScopeTest {
	@Test
	void cancellingOuterFutureAlsoCancelsItsProtocolChildren() {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		final CompletableFuture<Void> aggregate = new CompletableFuture<>();
		try {
			final var parent = PlcRequestBudget.submit(System.nanoTime() + TimeUnit.SECONDS.toNanos(2), () -> {
				for (int index = 0; index < 2; index++) {
					manager.track(() -> {
						manager.startRequest().submit(() -> {
						});
						return new CompletableFuture<>();
					}, 2000);
				}
				return aggregate;
			});
			assertEquals(2, manager.pendingCount());
			parent.cancel(false);
			assertEquals(0, manager.pendingCount());
			assertEquals(0, manager.pendingResponseCount());
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void asynchronousChildWriteIsCancelledWithItsParent() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		final AtomicReference<RequestTransaction> header = new AtomicReference<>();
		final AtomicReference<RequestTransaction> child = new AtomicReference<>();
		final AtomicReference<Consumer<Object>> callback = new AtomicReference<>();
		final AtomicInteger writes = new AtomicInteger();
		final CountDownLatch sent = new CountDownLatch(1);
		final CompletableFuture<Void> wire = new CompletableFuture<>();
		try {
			final CompletableFuture<Void> root = manager.track(() -> {
				final RequestTransaction tx = manager.startRequest();
				header.set(tx);
				tx.submit(() -> {
					callback.set(ManagedRequestTransactionManager.bindCurrentCallback(ignored -> wire.complete(null)));
					sent.countDown();
				});
				return wire.thenCompose(ignored -> {
					final CompletableFuture<Void> reply = new CompletableFuture<>();
					final RequestTransaction next = manager.startRequest();
					child.set(next);
					next.submit(() -> {
						writes.incrementAndGet();
						next.endRequest();
						reply.complete(null);
					});
					return reply;
				});
			}, 500);
			assertTrue(sent.await(2, TimeUnit.SECONDS));
			final RequestTransaction blocker = manager.startRequest();
			blocker.submit(() -> {
			});
			header.get().endRequest();
			callback.get().accept(null);
			assertThrows(ExecutionException.class, () -> root.get(2, TimeUnit.SECONDS));
			assertTrue(child.get().getCompletionFuture().isCancelled());
			assertEquals(1, manager.pendingCount());
			blocker.endRequest();
			assertEquals(0, manager.pendingCount());
			assertEquals(0, writes.get());
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void synchronousVoidOperationFailureCannotTurnIntoSuccess() {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final CompletableFuture<Void> response = manager.trackOperation(() -> {
				final RequestTransaction transaction = manager.startRequest();
				transaction.failRequest(new IllegalArgumentException("bad operation"));
			}, 1000);
			assertTrue(response.isCompletedExceptionally());
		} finally {
			manager.shutdown();
		}
	}
}
