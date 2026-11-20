package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.s7.readwrite.*;
import org.apache.plc4x.java.s7.readwrite.tag.S7SubscriptionTag;
import org.apache.plc4x.java.s7.readwrite.types.S7SubscriptionType;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.messages.DefaultPlcReadRequest;
import org.apache.plc4x.java.spi.messages.DefaultPlcWriteRequest;
import org.apache.plc4x.java.spi.messages.DefaultPlcSubscriptionRequest;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@Timeout(10)
class ManagedS7ProtocolLogicTest {
	@Test
	void actualMalformedReadAndWriteResponsesExposeTheirCause() throws Exception {
		final Stub logic = new Stub();
		try {
			final DefaultPlcReadRequest read = mock(DefaultPlcReadRequest.class);
			when(read.getTagNames()).thenReturn(new LinkedHashSet<>());
			final DefaultPlcWriteRequest write = mock(DefaultPlcWriteRequest.class);
			when(write.getTagNames()).thenReturn(new LinkedHashSet<>());
			final ExecutionException readFailure = assertThrows(ExecutionException.class, () -> logic.read(read).get(1, TimeUnit.SECONDS));
			final ExecutionException writeFailure = assertThrows(ExecutionException.class, () -> logic.write(write).get(1, TimeUnit.SECONDS));
			assertFalse(readFailure.getCause() instanceof TimeoutException);
			assertFalse(writeFailure.getCause() instanceof TimeoutException);
			assertNotNull(readFailure.getCause());
			assertNotNull(writeFailure.getCause());
		} finally {
			logic.dispose();
		}
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	void repeatedNativeSubscriptionTimeoutDoesNotOccupyDecoderThreads() throws Exception {
		final Stub logic = new Stub();
		final ConversationContext<TPKTPacket> context = mock(ConversationContext.class);
		final ConversationContext.SendRequestContext send = mock(ConversationContext.SendRequestContext.class, RETURNS_SELF);
		when(context.sendRequest(any())).thenReturn(send);
		final BlockingQueue<Consumer<TimeoutException>> callbacks = new ArrayBlockingQueue<>(20);
		when(send.onTimeout(any())).thenAnswer(invocation -> {
			callbacks.add(invocation.getArgument(0));
			return send;
		});
		logic.setConversationContext(context);
		final DefaultPlcSubscriptionRequest request = mock(DefaultPlcSubscriptionRequest.class);
		final DefaultPlcSubscriptionTag subscription = mock(DefaultPlcSubscriptionTag.class);
		final S7SubscriptionTag tag = mock(S7SubscriptionTag.class);
		when(tag.getTagType()).thenReturn(S7SubscriptionType.EVENT_SUBSCRIPTION);
		when(subscription.getTag()).thenReturn(tag);
		when(request.getTags()).thenReturn(List.of(subscription));
		try {
			for (int index = 0; index < 12; index++) {
				final var response = logic.subscribe(request);
				final Consumer<TimeoutException> callback = callbacks.poll(1, TimeUnit.SECONDS);
				assertNotNull(callback);
				callback.accept(new TimeoutException("peer silent"));
				assertThrows(ExecutionException.class, () -> response.get(1, TimeUnit.SECONDS));
			}
			final ThreadPoolExecutor decoder = (ThreadPoolExecutor) org.springframework.test.util.ReflectionTestUtils.getField(logic, "decoder");
			assertEquals(0, decoder.getActiveCount());
			assertEquals(0, decoder.getQueue().size());
		} finally {
			logic.dispose();
		}
	}

	private static final class Stub extends ManagedS7ProtocolLogic {
		@Override
		@SuppressWarnings("unchecked")
		protected <T> T access(final String name, final Object... arguments) {
			return switch (name) {
				case "isConnected", "isFeatureSupported" -> (T) Boolean.TRUE;
				case "getTpduId" -> (T) Integer.valueOf(1);
				case "encodeEventSubscriptionRequest" -> (T) mock(S7Message.class);
				case "performOrdinaryReadRequest", "performOrdinaryWriteRequest" ->
						(T) CompletableFuture.completedFuture(mock(S7Message.class));
				default -> super.access(name, arguments);
			};
		}

		private void dispose() {
			((ManagedRequestTransactionManager) org.springframework.test.util.ReflectionTestUtils.getField(this, "transactions")).shutdown();
			((ThreadPoolExecutor) org.springframework.test.util.ReflectionTestUtils.getField(this, "decoder")).shutdownNow();
		}
	}
}
