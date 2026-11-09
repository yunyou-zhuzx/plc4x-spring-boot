package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.api.metadata.PlcConnectionMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PlcLifecycleTest {
	private DefaultPlcDriverManager manager;
	private PlcDriver driver;
	private PlcConnection connection;
	private ConnectionConfig config;

	@BeforeEach
	void setUp() throws Exception {
		manager = mock(DefaultPlcDriverManager.class);
		driver = mock(PlcDriver.class);
		connection = mock(PlcConnection.class);
		config = mock(ConnectionConfig.class);
		when(config.address()).thenReturn("s7://localhost");
		when(config.protocol()).thenReturn(PlcProtocol.values()[0]);
		when(config.requestTimeoutMillis()).thenReturn(1L);
		when(config.reconnectIntervalMillis()).thenReturn(5000L);
		when(manager.getDriverForUrl(anyString())).thenReturn(driver);
		when(driver.getConnection(anyString())).thenReturn(connection);
		final PlcConnectionMetadata metadata = mock(PlcConnectionMetadata.class);
		when(metadata.isReadSupported()).thenReturn(true);
		when(metadata.isWriteSupported()).thenReturn(true);
		when(connection.getMetadata()).thenReturn(metadata);
	}

	@Test
	void failedHandshakeClosesUnconnectedCandidate() throws Exception {
		doThrow(new PlcConnectionException("handshake failed")).when(connection).connect();
		assertThrows(PlcConnectionException.class, () -> new PLC(config, manager));
		verify(connection).close();
	}

	@Test
	void offlineConnectionIsClosedAndCooldownPreventsReconnectStorm() throws Exception {
		prepareRequests(CompletableFuture.failedFuture(new IllegalStateException("disconnected")), new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> plc.read("tag", "address", Integer.class));
		assertFalse(plc.checkAndReconnectConnection());
		for (int i = 0; i < 100; i++) {
			assertFalse(plc.checkAndReconnectConnection());
		}
		verify(driver).getConnection(anyString());
		verify(connection).close();
	}

	@Test
	void closeIsIdempotentAndPreventsResurrection() throws Exception {
		final PLC plc = new PLC(config, manager);
		PlcManager.close(plc);
		plc.close();
		assertFalse(plc.checkAndReconnectConnection());
		assertFalse(plc.reconnectWithRetry());
		assertThrows(PlcConnectionException.class, plc::createNewConnection);
		verify(connection).close();
		verify(driver).getConnection(anyString());
	}

	@Test
	void failedCloseRetainsOwnershipAndBlocksReplacement() throws Exception {
		final PLC plc = new PLC(config, manager);
		doThrow(new PlcConnectionException("close failed")).when(connection).close();
		assertThrows(PlcConnectionException.class, plc::createNewConnection);
		assertSame(connection, plc.getPlcConnection());
		verify(driver).getConnection(anyString());
		doNothing().when(connection).close();
		plc.close();
		assertNull(plc.getPlcConnection());
	}

	@ParameterizedTest
	@ValueSource(strings = {"read", "batchRead", "write", "batchWrite"})
	void timeoutCancelsRequestAndClosesConnection(final String operation) throws Exception {
		final CompletableFuture<PlcReadResponse> readFuture = new CompletableFuture<>();
		final CompletableFuture<PlcWriteResponse> writeFuture = new CompletableFuture<>();
		prepareRequests(readFuture, writeFuture);
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> invoke(plc, operation));
		assertTrue(operation.contains("rite") ? writeFuture.isCancelled() : readFuture.isCancelled());
		verify(connection).close();
		assertNull(plc.getPlcConnection());
		assertFalse(plc.checkAndReconnectConnection());
	}

	@Test
	void exceptionalReadClosesConnection() throws Exception {
		prepareRequests(CompletableFuture.failedFuture(new IllegalStateException("disconnected")), new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection).close();
	}

	@Test
	void interruptedReadRestoresInterruptAndReleasesConnection() throws Exception {
		final CompletableFuture<PlcReadResponse> future = new CompletableFuture<>();
		prepareRequests(future, new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		Thread.currentThread().interrupt();
		try {
			assertThrows(InterruptedException.class, () -> plc.read("tag", "address", Integer.class));
			assertTrue(Thread.currentThread().isInterrupted());
			assertTrue(future.isCancelled());
			verify(connection).close();
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void concurrentReconnectCreatesOnlyOneReplacement() throws Exception {
		final PLC plc = new PLC(config, manager);
		when(config.reconnectIntervalMillis()).thenReturn(0L);
		plc.destroyStaleConnection();
		final PlcConnection replacement = mock(PlcConnection.class);
		when(driver.getConnection(anyString())).thenReturn(replacement);
		final CountDownLatch start = new CountDownLatch(1);
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(4));
		try {
			final List<Future<Boolean>> tasks = new java.util.ArrayList<>(4);
			for (int i = 0; i < 4; i++) {
				tasks.add(executor.submit(() -> {
					start.await();
					return plc.checkAndReconnectConnection();
				}));
			}
			start.countDown();
			for (Future<Boolean> task : tasks) {
				assertTrue(task.get(2, TimeUnit.SECONDS));
			}
			verify(connection).close();
			verify(driver, times(2)).getConnection(anyString());
		} finally {
			executor.shutdownNow();
			plc.close();
		}
	}

	@Test
	void repeatedHandshakeFailuresReleaseEveryCandidate() throws Exception {
		doThrow(new PlcConnectionException("handshake failed")).when(connection).connect();
		for (int i = 0; i < 100; i++) {
			assertThrows(PlcConnectionException.class, () -> new PLC(config, manager));
		}
		verify(connection, times(100)).close();
	}

	@Test
	void synchronousRequestFailureClosesConnection() throws Exception {
		prepareRequests(new CompletableFuture<>(), new CompletableFuture<>());
		final PlcReadRequest request = connection.readRequestBuilder().build();
		doThrow(new IllegalStateException("submission failed")).when(request).execute();
		final PLC plc = new PLC(config, manager);
		assertThrows(IllegalStateException.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection).close();
	}

	/**
	 * 驱动无论误报在线还是离线，都由读写结果驱动失效与恢复。
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void driverStatusDoesNotControlLifecycle(final boolean reportedStatus) throws Exception {
		when(connection.isConnected()).thenReturn(reportedStatus);
		prepareRequests(CompletableFuture.failedFuture(new IllegalStateException("disconnected")), new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		try {
			assertTrue(plc.isConnected());
			assertTrue(plc.checkAndReconnectConnection());
			when(config.reconnectIntervalMillis()).thenReturn(0L);
			assertThrows(Exception.class, () -> plc.read("tag", "address", Integer.class));
			assertFalse(plc.isConnected());
			verify(connection).close();
			assertTrue(plc.checkAndReconnectConnection());
			assertTrue(plc.isConnected());
			verify(driver, times(2)).getConnection(anyString());
			verify(connection, never()).isConnected();
		} finally {
			plc.close();
		}
	}

	/**
	 * 显式关闭失败必须让调用方感知，同时保留资源以便再次清理。
	 */
	@Test
	void permanentCloseReportsFailureAndCanBeRetried() throws Exception {
		final PLC plc = new PLC(config, manager);
		doThrow(new PlcConnectionException("close failed")).when(connection).close();
		final IllegalStateException failure = assertThrows(IllegalStateException.class, plc::close);
		assertInstanceOf(PlcConnectionException.class, failure.getCause());
		assertSame(connection, plc.getPlcConnection());
		assertFalse(plc.checkAndReconnectConnection());
		verify(connection, times(2)).close();
		doNothing().when(connection).close();
		plc.close();
		assertNull(plc.getPlcConnection());
	}

	private void prepareRequests(final CompletableFuture<PlcReadResponse> readFuture,
								 final CompletableFuture<PlcWriteResponse> writeFuture) {
		final PlcReadRequest.Builder readBuilder = mock(PlcReadRequest.Builder.class);
		final PlcReadRequest read = mock(PlcReadRequest.class);
		when(connection.readRequestBuilder()).thenReturn(readBuilder);
		when(readBuilder.build()).thenReturn(read);
		doReturn(readFuture).when(read).execute();
		final PlcWriteRequest.Builder writeBuilder = mock(PlcWriteRequest.Builder.class);
		final PlcWriteRequest write = mock(PlcWriteRequest.class);
		when(connection.writeRequestBuilder()).thenReturn(writeBuilder);
		when(writeBuilder.build()).thenReturn(write);
		doReturn(writeFuture).when(write).execute();
	}

	private void invoke(final PLC plc, final String operation) {
		switch (operation) {
			case "read" -> plc.read("tag", "address", Integer.class);
			case "batchRead" -> plc.read(List.of(), null);
			case "write" -> plc.write("tag", "address", 1);
			case "batchWrite" -> plc.write(List.of());
			default -> throw new IllegalArgumentException(operation);
		}
	}
}