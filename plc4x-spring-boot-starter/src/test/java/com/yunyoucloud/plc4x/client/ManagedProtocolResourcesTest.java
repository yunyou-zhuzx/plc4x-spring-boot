package com.yunyoucloud.plc4x.client;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.Plc4xNettyWrapper;
import org.apache.plc4x.java.spi.Plc4xProtocolBase;
import org.apache.plc4x.java.spi.connection.PlcTagHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 使用真实 SPI 响应匹配器验证取消、超时及断线不会留下报文等待器。
 */
@Timeout(15)
class ManagedProtocolResourcesTest {

	@Test
	void realSpiResponseCarriesCancellationIntoNextStage() throws Exception {
		try (final Fixture fixture = new Fixture()) {
			final CountDownLatch sent = new CountDownLatch(1);
			final CompletableFuture<String> header = new CompletableFuture<>();
			final java.util.concurrent.atomic.AtomicReference<org.apache.plc4x.java.spi.transaction.RequestTransactionManager.RequestTransaction> child = new java.util.concurrent.atomic.AtomicReference<>();
			final java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
			final CompletableFuture<String> root = fixture.manager.track(() -> {
				final var transaction = fixture.manager.startRequest();
				transaction.submit(() -> {
					fixture.protocol.getConversationContext().sendRequest("header")
							.expectResponse(String.class, Duration.ofSeconds(5))
							.onTimeout(header::completeExceptionally)
							.handle(value -> {
								transaction.endRequest();
								header.complete(value);
							});
					sent.countDown();
				});
				return header.thenCompose(value -> {
					final CompletableFuture<String> result = new CompletableFuture<>();
					final var next = fixture.manager.startRequest();
					child.set(next);
					next.submit(() -> {
						writes.incrementAndGet();
						next.endRequest();
						result.complete("written");
					});
					return result;
				});
			}, 500);
			assertTrue(sent.await(2, TimeUnit.SECONDS));
			final var blocker = fixture.manager.startRequest();
			blocker.submit(() -> {
			});
			fixture.channel.writeInbound("header-ok");
			assertThrows(ExecutionException.class, () -> root.get(2, TimeUnit.SECONDS));
			assertTrue(child.get().getCompletionFuture().isCancelled());
			assertEquals(1, fixture.manager.pendingCount());
			blocker.endRequest();
			assertEquals(0, fixture.manager.pendingCount());
			assertEquals(0, writes.get());
			assertEquals(0, fixture.resources.pendingCount());
		}
	}

	@Test
	void repeatedCancellationRemovesWireHandlersBeforeNativeTimeout() throws Exception {
		try (final Fixture fixture = new Fixture()) {
			for (int index = 0; index < 100; index++) {
				final CompletableFuture<String> response = fixture.request(5000);
				assertEquals(1, fixture.resources.pendingCount());
				assertTrue(response.cancel(false));
				assertEquals(0, fixture.resources.pendingCount());
				assertEquals(0, fixture.manager.pendingCount());
			}
			final CompletableFuture<String> healthy = fixture.request(5000);
			fixture.channel.writeInbound("ok");
			assertEquals("ok", healthy.get(2, TimeUnit.SECONDS));
			assertEquals(0, fixture.resources.pendingCount());
			assertTrue(fixture.channel.isActive());
		}
	}

	@Test
	void outerDeadlineReleasesLongerWireTimeoutWithoutClosingTransport() throws Exception {
		try (final Fixture fixture = new Fixture()) {
			final CompletableFuture<String> response = fixture.request(300);
			assertThrows(ExecutionException.class, () -> response.get(2, TimeUnit.SECONDS));
			assertEquals(0, fixture.resources.pendingCount());
			assertTrue(fixture.channel.isActive());
		}
	}

	@Test
	void remoteChannelCloseClearsWireRegistry() throws Exception {
		try (final Fixture fixture = new Fixture()) {
			fixture.request(5000);
			fixture.channel.close().sync();
			assertEquals(0, fixture.resources.pendingCount());
		}
	}

	private static final class Fixture implements AutoCloseable {
		private final EmbeddedChannel channel = new EmbeddedChannel();
		private final ManagedRequestTransactionManager manager = new ManagedRequestTransactionManager();
		private final TestProtocol protocol = new TestProtocol();
		private final ManagedProtocolResources resources;

		private Fixture() {
			channel.pipeline().addLast(new Plc4xNettyWrapper<>(channel.pipeline(), false, protocol, null, String.class));
			resources = ManagedProtocolResources.install(channel.pipeline());
		}

		private CompletableFuture<String> request(final long deadlineMillis) throws InterruptedException {
			final CountDownLatch sent = new CountDownLatch(1);
			final CompletableFuture<String> response = manager.track(() -> {
				final CompletableFuture<String> result = new CompletableFuture<>();
				final var transaction = manager.startRequest();
				transaction.submit(() -> {
					protocol.getConversationContext().sendRequest("request")
							.expectResponse(String.class, Duration.ofSeconds(10))
							.onError((request, failure) -> result.completeExceptionally(failure))
							.onTimeout(result::completeExceptionally)
							.handle(result::complete);
					sent.countDown();
				});
				return result;
			}, deadlineMillis);
			assertTrue(sent.await(2, TimeUnit.SECONDS));
			return response;
		}

		@Override
		public void close() {
			manager.shutdown();
			channel.finishAndReleaseAll();
		}
	}

	private static final class TestProtocol extends Plc4xProtocolBase<String> {
		@Override
		public PlcTagHandler getTagHandler() {
			throw new UnsupportedOperationException("No tags in SPI lifecycle test");
		}

		@Override
		public void close(final ConversationContext<String> context) {
		}
	}
}
