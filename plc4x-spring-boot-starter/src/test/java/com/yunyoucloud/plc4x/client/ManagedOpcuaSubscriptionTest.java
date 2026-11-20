package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcUnsubscriptionRequest;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.types.PlcSubscriptionType;
import org.apache.plc4x.java.opcua.config.OpcuaConfiguration;
import org.apache.plc4x.java.opcua.context.Conversation;
import org.apache.plc4x.java.opcua.context.SecureChannel;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.opcua.tag.OpcuaTag;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 直接覆盖服务器离线、删除失败和晚到响应，不依赖现场 OPC UA 服务。
 */
@Timeout(10)
class ManagedOpcuaSubscriptionTest {

	private ManagedOpcuaProtocolLogic protocol;
	private ManagedRequestTransactionManager transactions;
	private Conversation conversation;
	private PlcSubscriptionRequest request;
	private CreateMonitoredItemsResponse monitored;

	@BeforeEach
	void setUp() {
		protocol = new ManagedOpcuaProtocolLogic();
		transactions = new ManagedRequestTransactionManager();
		conversation = mock(Conversation.class);
		final OpcuaConfiguration configuration = mock(OpcuaConfiguration.class);
		when(configuration.getRequestTimeout()).thenReturn(500L);
		protocol.setConfiguration(configuration);
		ReflectionTestUtils.setField(protocol, "conversation", conversation);
		request = mock(PlcSubscriptionRequest.class);
		when(request.getTagNames()).thenReturn(new LinkedHashSet<>(List.of("value")));
		final DefaultPlcSubscriptionTag tag = mock(DefaultPlcSubscriptionTag.class);
		when(tag.getTag()).thenReturn(OpcuaTag.of("ns=2;i=1001"));
		when(tag.getDuration()).thenReturn(java.util.Optional.of(Duration.ofMillis(100000)));
		when(tag.getPlcSubscriptionType()).thenReturn(PlcSubscriptionType.CYCLIC);
		when(request.getTag("value")).thenReturn(tag);
		monitored = mock(CreateMonitoredItemsResponse.class);
		final MonitoredItemCreateResult result = mock(MonitoredItemCreateResult.class);
		final StatusCode good = mock(StatusCode.class);
		when(good.getStatusCode()).thenReturn(0L);
		when(result.getStatusCode()).thenReturn(good);
		when(monitored.getResults()).thenReturn(List.of(result));
		when(conversation.submit(any(CreateMonitoredItemsRequest.class), eq(CreateMonitoredItemsResponse.class)))
				.thenReturn(CompletableFuture.completedFuture(monitored));
	}

	@AfterEach
	void cleanUp() {
		protocol.transportClosed();
		transactions.shutdown();
	}

	@Test
	void slowPublishDoesNotAccumulateAndStopCancelsItsTransaction() throws Exception {
		final ManagedOpcuaSubscription handle = handle();
		final RequestHeader header = mock(RequestHeader.class);
		when(conversation.createRequestHeader(anyLong())).thenReturn(header);
		when(conversation.submit(any(PublishRequest.class), eq(PublishResponse.class)))
				.thenReturn(new CompletableFuture<>());
		try {
			ReflectionTestUtils.invokeMethod(handle, "publish");
			verify(conversation, timeout(1000)).submit(any(PublishRequest.class), eq(PublishResponse.class));
			for (int index = 0; index < 20; index++) {
				ReflectionTestUtils.invokeMethod(handle, "publish");
			}
			verify(conversation).submit(any(PublishRequest.class), eq(PublishResponse.class));
			assertEquals(1, transactions.pendingCount());
			handle.stopLocal();
			assertEquals(0, transactions.pendingCount());
		} finally {
			handle.stopLocal();
		}
	}

	@Test
	void deleteFailureStopsLocalTaskAndReturnsFailedFuture() throws Exception {
		final ManagedOpcuaSubscription handle = handle();
		try {
			handle.onSubscribeCreateMonitoredItemsRequest().get(1, TimeUnit.SECONDS);
			assertTrue(handle.isPublishing());
			when(conversation.submit(any(DeleteSubscriptionsRequest.class), eq(DeleteSubscriptionsResponse.class)))
					.thenReturn(CompletableFuture.failedFuture(new IllegalStateException("BadInternalError")));
			final PlcUnsubscriptionRequest unsubscribe = mock(PlcUnsubscriptionRequest.class);
			when(unsubscribe.getSubscriptionHandles()).thenReturn(List.of(handle));
			final var response = protocol.unsubscribe(unsubscribe);
			assertNotNull(response);
			assertFalse(handle.isPublishing());
			assertThrows(ExecutionException.class, () -> response.get(1, TimeUnit.SECONDS));
		} finally {
			handle.stopLocal();
		}
	}

	@Test
	void missingDeleteResponseStillStopsLocalTaskImmediately() throws Exception {
		final ManagedOpcuaSubscription handle = handle();
		try {
			handle.onSubscribeCreateMonitoredItemsRequest().get(1, TimeUnit.SECONDS);
			when(conversation.submit(any(DeleteSubscriptionsRequest.class), eq(DeleteSubscriptionsResponse.class)))
					.thenReturn(new CompletableFuture<>());
			final CompletableFuture<Void> stopped = handle.stop();
			assertFalse(handle.isPublishing());
			assertThrows(ExecutionException.class, () -> stopped.get(2, TimeUnit.SECONDS));
			final CompletableFuture<Void> retry = handle.stop();
			assertNotSame(stopped, retry);
			assertSame(retry, handle.stop());
			verify(conversation, timeout(1000).times(2)).submit(any(DeleteSubscriptionsRequest.class), eq(DeleteSubscriptionsResponse.class));
		} finally {
			handle.stopLocal();
		}
	}

	@Test
	void lateMonitorResponseCannotRestartStoppedSubscription() {
		final ManagedOpcuaSubscription handle = handle();
		final CompletableFuture<CreateMonitoredItemsResponse> delayed = new CompletableFuture<>();
		when(conversation.submit(any(CreateMonitoredItemsRequest.class), eq(CreateMonitoredItemsResponse.class)))
				.thenReturn(delayed);
		final var creating = handle.onSubscribeCreateMonitoredItemsRequest();
		handle.stopLocal();
		delayed.complete(monitored);
		assertTrue(creating.isCompletedExceptionally());
		assertFalse(handle.isPublishing());
	}

	@Test
	void failedDeletionCanRetryAndAlreadyAbsentSubscriptionIsSuccessful() throws Exception {
		final ManagedOpcuaSubscription handle = handle();
		final DeleteSubscriptionsResponse deleted = mock(DeleteSubscriptionsResponse.class);
		final StatusCode absent = mock(StatusCode.class);
		when(absent.getStatusCode()).thenReturn(OpcuaStatusCode.BadSubscriptionIdInvalid.getValue());
		when(deleted.getResults()).thenReturn(List.of(absent));
		when(conversation.submit(any(DeleteSubscriptionsRequest.class), eq(DeleteSubscriptionsResponse.class)))
				.thenReturn(CompletableFuture.failedFuture(new IllegalStateException("BadInternalError")))
				.thenReturn(CompletableFuture.completedFuture(deleted));
		try {
			final CompletableFuture<Void> first = handle.stop();
			assertThrows(ExecutionException.class, () -> first.get(1, TimeUnit.SECONDS));
			final CompletableFuture<Void> retry = handle.stop();
			retry.get(1, TimeUnit.SECONDS);
			assertSame(retry, handle.stop());
			verify(conversation, times(2)).submit(any(DeleteSubscriptionsRequest.class), eq(DeleteSubscriptionsResponse.class));
			assertFalse(handle.isPublishing());
		} finally {
			handle.stopLocal();
		}
	}

	@Test
	void registrationUnregistersWithoutRecursionAndPreservesOtherRegistrations() {
		final ManagedOpcuaSubscription handle = handle();
		final Consumer<PlcSubscriptionEvent> consumer = event -> {
		};
		final PlcConsumerRegistration first = handle.register(consumer);
		final PlcConsumerRegistration second = handle.register(consumer);
		assertEquals(1, Plc4xOpcuaAccess.consumers(handle).size());
		assertDoesNotThrow(first::unregister);
		assertEquals(1, Plc4xOpcuaAccess.consumers(handle).size());
		assertDoesNotThrow(() -> protocol.unregister(second));
		assertEquals(0, Plc4xOpcuaAccess.consumers(handle).size());
		assertDoesNotThrow(second::unregister);
		handle.stopLocal();
	}

	@Test
	void closeImmediatelyCancelsKeepAliveAndClearsCallbacks() {
		final SecureChannel channel = mock(SecureChannel.class);
		final ScheduledFuture<?> keepAlive = mock(ScheduledFuture.class);
		ReflectionTestUtils.setField(channel, "keepAlive", keepAlive);
		ReflectionTestUtils.setField(protocol, "channel", channel);
		protocol.transportClosed();
		verify(keepAlive).cancel(false);
	}

	@Test
	void repeatedTransportCleanupCancelsLateKeepAliveAssignment() {
		final SecureChannel channel = mock(SecureChannel.class);
		ReflectionTestUtils.setField(protocol, "channel", channel);
		protocol.transportClosed();
		final ScheduledFuture<?> late = mock(ScheduledFuture.class);
		ReflectionTestUtils.setField(channel, "keepAlive", late);
		protocol.transportClosed();
		verify(late).cancel(false);
	}

	@Test
	void subscribeUsesManagedHandleAndCloseRemovesIt() throws Exception {
		final CreateSubscriptionResponse response = mock(CreateSubscriptionResponse.class);
		when(response.getSubscriptionId()).thenReturn(7L);
		when(response.getRevisedPublishingInterval()).thenReturn(100000d);
		when(conversation.submit(any(CreateSubscriptionRequest.class), eq(CreateSubscriptionResponse.class)))
				.thenReturn(CompletableFuture.completedFuture(response));
		final var subscribed = protocol.subscribe(request).get(2, TimeUnit.SECONDS);
		final ManagedOpcuaSubscription handle = (ManagedOpcuaSubscription) subscribed.getSubscriptionHandle("value");
		assertTrue(handle.isPublishing());
		handle.register(event -> {
		});
		protocol.transportClosed();
		assertFalse(handle.isPublishing());
		assertEquals(0, Plc4xOpcuaAccess.consumers(handle).size());
	}

	@Test
	void rejectedMonitorDoesNotStartPublisher() {
		final MonitoredItemCreateResult result = mock(MonitoredItemCreateResult.class);
		final StatusCode bad = mock(StatusCode.class);
		when(bad.getStatusCode()).thenReturn(0x80020000L);
		when(result.getStatusCode()).thenReturn(bad);
		when(monitored.getResults()).thenReturn(List.of(result));
		final ManagedOpcuaSubscription handle = handle();
		assertTrue(handle.onSubscribeCreateMonitoredItemsRequest().isCompletedExceptionally());
		assertFalse(handle.isPublishing());
		handle.stopLocal();
	}

	private ManagedOpcuaSubscription handle() {
		return new ManagedOpcuaSubscription(protocol, transactions, conversation,
				new ManagedOpcuaSubscription.Spec(request, 7L, 100000));
	}
}
