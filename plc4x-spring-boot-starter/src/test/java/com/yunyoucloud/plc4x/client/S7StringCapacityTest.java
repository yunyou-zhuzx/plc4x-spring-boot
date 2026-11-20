package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.api.value.PlcValue;
import org.apache.plc4x.java.s7.readwrite.*;
import org.apache.plc4x.java.s7.readwrite.tag.*;
import org.apache.plc4x.java.spi.messages.DefaultPlcWriteRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(10)
class S7StringCapacityTest {
	@Test
	void fixedLengthStringRejectsOverflowIncludingMultibyteUtf8() throws Exception {
		final Stub logic = new Stub();
		final DefaultPlcWriteRequest request = mock(DefaultPlcWriteRequest.class);
		when(request.getTagNames()).thenReturn(new LinkedHashSet<>(List.of("text")));
		when(request.getTag("text")).thenReturn(new S7StringFixedLengthTag(TransportSize.STRING, MemoryArea.DATA_BLOCKS, 1, 40, (byte) 0, 1, 2));
		final PlcValue value = mock(PlcValue.class);
		when(request.getPlcValue("text")).thenReturn(value);
		when(value.getIndex(0)).thenReturn(value);
		try {
			when(value.getString()).thenReturn("abc");
			assertThrows(ExecutionException.class, () -> logic.write(request).get(2, TimeUnit.SECONDS));
			when(value.getString()).thenReturn("中");
			assertThrows(ExecutionException.class, () -> logic.write(request).get(2, TimeUnit.SECONDS));
			assertNull(logic.sent);
		} finally {
			((ManagedRequestTransactionManager) ReflectionTestUtils.getField(logic, "transactions")).shutdown();
			((ThreadPoolExecutor) ReflectionTestUtils.getField(logic, "decoder")).shutdownNow();
		}
	}

	@Test
	void writeUsesMaximumCapacityAndOversizedValueNeverReachesWriteStage() throws Exception {
		final Stub logic = new Stub();
		final DefaultPlcWriteRequest request = mock(DefaultPlcWriteRequest.class);
		when(request.getTagNames()).thenReturn(new LinkedHashSet<>(List.of("text")));
		when(request.getTag("text")).thenReturn(logic.tag);
		final PlcValue value = mock(PlcValue.class);
		when(request.getPlcValue("text")).thenReturn(value);
		when(value.getIndex(0)).thenReturn(value);
		when(value.getString()).thenReturn("longer string");
		try {
			logic.write(request).get(2, TimeUnit.SECONDS);
			assertEquals(20, ((S7StringFixedLengthTag) logic.sent.getTag("text")).getStringLength());
			logic.sent = null;
			when(value.getString()).thenReturn("x".repeat(21));
			assertThrows(ExecutionException.class, () -> logic.write(request).get(2, TimeUnit.SECONDS));
			assertNull(logic.sent);
		} finally {
			((ManagedRequestTransactionManager) ReflectionTestUtils.getField(logic, "transactions")).shutdown();
			((ThreadPoolExecutor) ReflectionTestUtils.getField(logic, "decoder")).shutdownNow();
		}
	}

	private static final class Stub extends ManagedS7ProtocolLogic {
		private final S7StringVarLengthTag tag = S7StringVarLengthTag.of("%DB1:40:STRING");
		private DefaultPlcWriteRequest sent;

		@Override
		@SuppressWarnings("unchecked")
		protected <T> T access(final String name, final Object... arguments) {
			return switch (name) {
				case "isConnected" -> (T) Boolean.TRUE;
				case "getStringSizes" -> (T) CompletableFuture.completedFuture(Map.of(tag, new StringSizes(20, 3)));
				case "performOrdinaryWriteRequest" -> {
					sent = (DefaultPlcWriteRequest) arguments[0];
					yield (T) CompletableFuture.completedFuture(mock(S7Message.class));
				}
				case "decodeWriteResponse" -> (T) mock(PlcWriteResponse.class);
				default -> super.access(name, arguments);
			};
		}
	}
}
