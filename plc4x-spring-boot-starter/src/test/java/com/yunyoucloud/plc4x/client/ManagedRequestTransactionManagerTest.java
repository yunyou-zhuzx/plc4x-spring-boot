package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.spi.transaction.RequestTransactionManager.RequestTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证事务队列的终态、容量和排队时限，不使用现场设备。
 */
@Timeout(10)
class ManagedRequestTransactionManagerTest {

	@Test
	void timeoutReleasesSlotAndNextRequestRuns() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final CompletableFuture<Integer> first = pending(manager, new AtomicInteger(), 50);
			final CompletableFuture<Integer> second = manager.track(() -> {
				final CompletableFuture<Integer> response = new CompletableFuture<>();
				manager.startRequest().submit(() -> response.complete(42));
				return response;
			}, 2000);
			assertThrows(ExecutionException.class, () -> first.get(2, TimeUnit.SECONDS));
			assertEquals(42, second.get(2, TimeUnit.SECONDS));
			assertEquals(0, manager.pendingCount());
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void cancelledQueuedWriteIsNeverExecutedLater() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final AtomicInteger sent = new AtomicInteger();
			final CompletableFuture<Integer> wire = new CompletableFuture<>();
			final CompletableFuture<Integer> first = pending(manager, sent, 2000, wire);
			final CompletableFuture<Integer> second = pending(manager, sent, 2000);
			assertTrue(second.cancel(true));
			wire.complete(1);
			assertEquals(1, first.get(2, TimeUnit.SECONDS));
			assertTrue(second.isCancelled());
			assertEquals(0, manager.pendingCount());
			assertTrue(sent.get() <= 1);
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void queuedRequestExpiresBeforeSlotBecomesAvailable() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final AtomicInteger sent = new AtomicInteger();
			final CompletableFuture<Integer> wire = new CompletableFuture<>();
			final CompletableFuture<Integer> first = pending(manager, sent, 2000, wire);
			final CompletableFuture<Integer> second = pending(manager, sent, 30);
			assertThrows(ExecutionException.class, () -> second.get(2, TimeUnit.SECONDS));
			wire.complete(1);
			assertEquals(0, manager.pendingCount());
			assertTrue(sent.get() <= 1);
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void closeFailsRunningAndQueuedRequestsAndClearsQueue() {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		final CompletableFuture<Integer> first = pending(manager, new AtomicInteger(), 2000);
		final CompletableFuture<Integer> second = pending(manager, new AtomicInteger(), 2000);
		manager.shutdown();
		assertTrue(first.isCompletedExceptionally());
		assertTrue(second.isCompletedExceptionally());
		assertEquals(0, manager.pendingCount());
		assertTrue(pending(manager, new AtomicInteger(), 2000).isCompletedExceptionally());
	}

	@Test
	void queueIsBoundedEvenWhenPeerNeverResponds() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final List<CompletableFuture<Integer>> requests = new ArrayList<>();
			for (int index = 0; index < 64; index++) {
				requests.add(pending(manager, new AtomicInteger(), 5000));
			}
			final CompletableFuture<Integer> excess = pending(manager, new AtomicInteger(), 5000);
			final ExecutionException failure = assertThrows(ExecutionException.class, () -> excess.get(1, TimeUnit.SECONDS));
			assertInstanceOf(RejectedExecutionException.class, failure.getCause());
			assertEquals(64, manager.pendingCount());
			manager.shutdown();
			assertTrue(requests.stream().allMatch(CompletableFuture::isCompletedExceptionally));
			assertEquals(0, manager.pendingCount());
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void synchronousFailureAndLateDoubleCompletionAreSafe() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final AtomicReference<RequestTransaction> saved = new AtomicReference<>();
			final CompletableFuture<Integer> response = manager.track(() -> {
				final var transaction = manager.startRequest();
				saved.set(transaction);
				transaction.submit(() -> {
					throw new IllegalStateException("send failed");
				});
				return new CompletableFuture<>();
			}, 1000);
			assertThrows(ExecutionException.class, () -> response.get(2, TimeUnit.SECONDS));
			assertDoesNotThrow(() -> saved.get().endRequest());
			assertDoesNotThrow(() -> saved.get().failRequest(new IllegalStateException("late failure")));
			assertEquals(0, manager.pendingCount());
		} finally {
			manager.shutdown();
		}
	}

	@Test
	void shutdownDoesNotHoldQueueLockWhileRunningBusinessCallback() throws Exception {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		final Object businessLock = new Object();
		final CountDownLatch callbackEntered = new CountDownLatch(1);
		final CompletableFuture<Integer> response = pending(manager, new AtomicInteger(), 2000);
		response.whenComplete((value, failure) -> {
			callbackEntered.countDown();
			synchronized (businessLock) {
				assertEquals(0, manager.pendingCount());
			}
		});
		final Thread closing = new Thread(manager::shutdown, "transaction-test-close");
		try {
			synchronized (businessLock) {
				closing.start();
				assertTrue(callbackEntered.await(2, TimeUnit.SECONDS));
				assertEquals(0, manager.pendingCount());
			}
			closing.join(2000);
			assertFalse(closing.isAlive());
		} finally {
			manager.shutdown();
			closing.join(2000);
		}
	}

	private CompletableFuture<Integer> pending(final ManagedRequestTransactionManager manager,
											   final AtomicInteger sent, final long budget) {
		return pending(manager, sent, budget, new CompletableFuture<>());
	}

	private CompletableFuture<Integer> pending(final ManagedRequestTransactionManager manager,
											   final AtomicInteger sent, final long budget,
											   final CompletableFuture<Integer> response) {
		return manager.track(() -> {
			manager.startRequest().submit(sent::incrementAndGet);
			return response;
		}, budget);
	}

	@Test
	void incompleteDecoderResponsesAreBoundedAndFailedOnClose() {
		final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		try {
			final List<CompletableFuture<Integer>> responses = new ArrayList<>();
			for (int index = 0; index < 64; index++) {
				responses.add(manager.track(CompletableFuture::new, 5000));
			}
			assertTrue(manager.track(CompletableFuture::new, 5000).isCompletedExceptionally());
			assertEquals(64, manager.pendingResponseCount());
			manager.shutdown();
			assertTrue(responses.stream().allMatch(CompletableFuture::isCompletedExceptionally));
			assertEquals(0, manager.pendingResponseCount());
		} finally {
			manager.shutdown();
		}
	}
}
