package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.spi.Plc4xProtocolBase;
import org.apache.plc4x.java.spi.context.DriverContext;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ManagedS7OptimizerTest {
	@Test
	void cancellingAggregateCancelsAllSplitWrites() {
		final List<CompletableFuture<PlcWriteResponse>> sent = new ArrayList<>();
		final Plc4xProtocolBase<?> protocol = mock(Plc4xProtocolBase.class);
		when(protocol.write(any())).thenAnswer(invocation -> {
			final CompletableFuture<PlcWriteResponse> future = new CompletableFuture<>();
			sent.add(future);
			return future;
		});
		final var response = new Split().optimizedWrite(mock(PlcWriteRequest.class), protocol);
		assertEquals(2, sent.size());
		response.cancel(false);
		assertTrue(sent.stream().allMatch(CompletableFuture::isCancelled));
	}

	@Test
	void expiredBudgetAndInterruptPreventAnyNewSplitWrite() {
		final Plc4xProtocolBase<?> protocol = mock(Plc4xProtocolBase.class);
		final PlcWriteRequest request = mock(PlcWriteRequest.class);
		final var response = PlcRequestBudget.submit(System.nanoTime() - 1, () -> new Split().optimizedWrite(request, protocol));
		assertTrue(response.isCompletedExceptionally());
		Thread.currentThread().interrupt();
		try {
			assertTrue(new Split().optimizedWrite(request, protocol).isCompletedExceptionally());
			assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
		verify(protocol, never()).write(any());
	}

	private static final class Split extends ManagedS7Optimizer {
		@Override
		protected List<PlcWriteRequest> processWriteRequest(final PlcWriteRequest request, final DriverContext context) {
			return List.of(mock(PlcWriteRequest.class), mock(PlcWriteRequest.class));
		}
	}
}
