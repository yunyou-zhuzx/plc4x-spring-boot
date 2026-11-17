package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.exceptions.PlcProtocolException;
import org.apache.plc4x.java.spi.connection.ChannelExposingConnection;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.api.metadata.PlcConnectionMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
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
		prepareRequests(CompletableFuture.failedFuture(new ClosedChannelException()), new CompletableFuture<>());
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
	void timeoutCancelsRequestAndKeepsConnection(final String operation) throws Exception {
		final CompletableFuture<PlcReadResponse> readFuture = new CompletableFuture<>();
		final CompletableFuture<PlcWriteResponse> writeFuture = new CompletableFuture<>();
		prepareRequests(readFuture, writeFuture);
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> invoke(plc, operation));
		assertTrue(operation.contains("rite") ? writeFuture.isCancelled() : readFuture.isCancelled());
		verify(connection, never()).close();
		assertSame(connection, plc.getPlcConnection());
		assertTrue(plc.checkAndReconnectConnection());
		prepareSuccessfulRequests();
		assertDoesNotThrow(() -> invoke(plc, operation));
		verify(driver).getConnection(anyString());
	}

	@Test
	void disconnectedReadClosesConnection() throws Exception {
		prepareRequests(CompletableFuture.failedFuture(new ClosedChannelException()), new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection).close();
	}

	@Test
	void interruptedReadRestoresInterruptAndKeepsConnection() throws Exception {
		final CompletableFuture<PlcReadResponse> future = new CompletableFuture<>();
		prepareRequests(future, new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		Thread.currentThread().interrupt();
		try {
			assertThrows(InterruptedException.class, () -> plc.read("tag", "address", Integer.class));
			assertTrue(Thread.currentThread().isInterrupted());
			assertTrue(future.isCancelled());
			verify(connection, never()).close();
			assertSame(connection, plc.getPlcConnection());
			assertTrue(plc.isConnected());
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
	void synchronousRequestFailureKeepsConnection() throws Exception {
		prepareRequests(new CompletableFuture<>(), new CompletableFuture<>());
		final PlcReadRequest request = connection.readRequestBuilder().build();
		doThrow(new IllegalStateException("submission failed")).when(request).execute();
		final PLC plc = new PLC(config, manager);
		assertThrows(IllegalStateException.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection, never()).close();
		assertTrue(plc.isConnected());
	}

	/**
	 * 驱动无论误报在线还是离线，都由明确断链故障驱动失效与恢复。
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void driverStatusDoesNotControlLifecycle(final boolean reportedStatus) throws Exception {
		when(connection.isConnected()).thenReturn(reportedStatus);
		prepareRequests(CompletableFuture.failedFuture(new ClosedChannelException()), new CompletableFuture<>());
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

	/**
	 * 服务端明确拒绝请求时保留原始异常，后续请求继续复用同一连接。
	 */
	@ParameterizedTest
	@ValueSource(strings = {"read", "batchRead", "write", "batchWrite"})
	void serverFaultKeepsConnectionAndNextRequestSucceeds(final String operation) throws Exception {
		prepareTransport();
		final PlcProtocolException fault = new PlcProtocolException("Server returned error BadInternalError (0x80020000)");
		prepareRequests(CompletableFuture.failedFuture(fault), CompletableFuture.failedFuture(fault));
		final PLC plc = new PLC(config, manager);
		final Exception failure = assertThrows(Exception.class, () -> invoke(plc, operation));
		final Throwable execution = operation.contains("rite") ? failure.getCause() : failure;
		assertInstanceOf(ExecutionException.class, execution);
		assertSame(fault, execution.getCause());
		assertSame(connection, plc.getPlcConnection());
		assertTrue(plc.isConnected());
		assertTrue(plc.checkAndReconnectConnection());
		prepareSuccessfulRequests();
		assertDoesNotThrow(() -> invoke(plc, operation));
		verify(connection, never()).close();
		verify(driver).getConnection(anyString());
	}

	/**
	 * 取消、普通异常和套接字等待超时均不能单独作为断链依据。
	 */
	@ParameterizedTest
	@ValueSource(strings = {"cancelled", "socketTimeout", "unknown"})
	void requestFailureWithoutDisconnectionKeepsConnection(final String failureType) throws Exception {
		final Throwable failure = switch (failureType) {
			case "cancelled" -> new CancellationException();
			case "socketTimeout" -> new SocketTimeoutException();
			default -> new IllegalStateException("request failed");
		};
		prepareRequests(CompletableFuture.failedFuture(failure), new CompletableFuture<>());
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection, never()).close();
		assertTrue(plc.checkAndReconnectConnection());
	}

	/**
	 * 明确断链异常即使经过多层包装也必须释放旧连接。
	 */
	@ParameterizedTest
	@ValueSource(strings = {"closed", "eof", "reset", "synchronous"})
	void transportFailureClosesConnection(final String failureType) throws Exception {
		final Throwable failure = switch (failureType) {
			case "eof" -> new EOFException();
			case "reset" -> new SocketException("Connection reset");
			default -> new ClosedChannelException();
		};
		prepareRequests(CompletableFuture.failedFuture(new CompletionException(failure)), new CompletableFuture<>());
		if ("synchronous".equals(failureType)) {
			final PlcReadRequest request = connection.readRequestBuilder().build();
			doThrow(new CompletionException(failure)).when(request).execute();
		}
		final PLC plc = new PLC(config, manager);
		assertThrows(Exception.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection).close();
		assertNull(plc.getPlcConnection());
		assertFalse(plc.isConnected());
	}

	/**
	 * 请求过程中真实通道断开，即使请求只报告超时也需要清理。
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void timeoutWithDisconnectedTransportClosesConnection(final boolean open) throws Exception {
		final Channel channel = prepareTransport();
		prepareRequests(new CompletableFuture<>(), new CompletableFuture<>());
		final PlcReadRequest request = connection.readRequestBuilder().build();
		doAnswer(invocation -> {
			when(channel.isOpen()).thenReturn(open);
			when(channel.isActive()).thenReturn(false);
			return new CompletableFuture<PlcReadResponse>();
		}).when(request).execute();
		final PLC plc = new PLC(config, manager);
		assertThrows(RuntimeException.class, () -> plc.read("tag", "address", Integer.class));
		verify(connection).close();
		assertNull(plc.getPlcConnection());
	}

	/**
	 * 已关闭的真实通道在下一次访问前被清理，随后建立替代连接。
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void closedTransportReconnectsWithoutTrustingDriverStatus(final boolean driverClearedChannel) throws Exception {
		final Channel channel = prepareTransport();
		final PLC plc = new PLC(config, manager);
		when(channel.isOpen()).thenReturn(false);
		when(channel.isActive()).thenReturn(false);
		if (driverClearedChannel) {
			when(((ChannelExposingConnection) connection).getChannel()).thenReturn(null);
		}
		assertFalse(plc.isConnected());
		final PlcConnection replacement = mock(PlcConnection.class);
		when(driver.getConnection(anyString())).thenReturn(replacement);
		assertTrue(plc.checkAndReconnectConnection());
		assertSame(replacement, plc.getPlcConnection());
		verify(connection).close();
		verify(connection, never()).isConnected();
		verify(driver, times(2)).getConnection(anyString());
	}

	/**
	 * 虚拟通道的关闭状态不能用于推断 S7H 的真实传输状态。
	 */
	@Test
	void embeddedChannelDoesNotTriggerTransportCleanup() throws Exception {
		prepareTransport();
		final EmbeddedChannel embedded = new EmbeddedChannel();
		try {
			embedded.close();
			when(((ChannelExposingConnection) connection).getChannel()).thenReturn(embedded);
			prepareRequests(CompletableFuture.failedFuture(new PlcProtocolException("request failed")), new CompletableFuture<>());
			final PLC plc = new PLC(config, manager);
			assertThrows(ExecutionException.class, () -> plc.read("tag", "address", Integer.class));
			assertTrue(plc.isConnected());
			verify(connection, never()).close();
		} finally {
			embedded.finishAndReleaseAll();
		}
	}

	/**
	 * 提供可检查真实传输状态的连接替身。
	 */
	private Channel prepareTransport() throws PlcConnectionException {
		final PlcConnectionMetadata metadata = connection.getMetadata();
		connection = mock(PlcConnection.class, withSettings().extraInterfaces(ChannelExposingConnection.class));
		when(driver.getConnection(anyString())).thenReturn(connection);
		when(connection.getMetadata()).thenReturn(metadata);
		final Channel channel = mock(Channel.class);
		when(channel.eventLoop()).thenReturn(mock(io.netty.channel.EventLoop.class));
		when(channel.isOpen()).thenReturn(true);
		when(channel.isActive()).thenReturn(true);
		when(((ChannelExposingConnection) connection).getChannel()).thenReturn(channel);
		return channel;
	}

	/**
	 * 提供正常响应，用于验证失败后仍可复用连接。
	 */
	private void prepareSuccessfulRequests() {
		final PlcReadResponse read = mock(PlcReadResponse.class);
		when(read.getResponseCode("tag")).thenReturn(org.apache.plc4x.java.api.types.PlcResponseCode.OK);
		when(read.getInteger("tag")).thenReturn(42);
		final PlcWriteResponse write = mock(PlcWriteResponse.class);
		when(write.getResponseCode("tag")).thenReturn(org.apache.plc4x.java.api.types.PlcResponseCode.OK);
		prepareRequests(CompletableFuture.completedFuture(read), CompletableFuture.completedFuture(write));
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
			case "batchWrite" -> {
				final com.yunyoucloud.plc4x.core.PlcParseData item = new com.yunyoucloud.plc4x.core.PlcParseData();
				item.getRequestItem().setTagName("tag");
				item.getRequestItem().setAddress("address");
				item.getResponseItem().setValue(1);
				plc.write(List.of(item));
			}
			default -> throw new IllegalArgumentException(operation);
		}
	}

	@Test
	void lazyClientKeepsOwnershipAcrossFailedFirstHandshake() throws Exception {
		final PLC plc = new PLC(config, manager, false);
		verifyNoInteractions(driver);
		doThrow(new PlcConnectionException("handshake failed")).when(connection).connect();
		doThrow(new PlcConnectionException("cleanup failed")).when(connection).close();
		assertFalse(plc.checkAndReconnectConnection());
		assertSame(connection, plc.getPlcConnection());
		assertThrows(IllegalStateException.class, plc::close);
		doNothing().when(connection).close();
		plc.close();
		assertNull(plc.getPlcConnection());
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void partialOrMissingFieldMustRejectWholeSample(final boolean missing) throws Exception {
		final PlcReadResponse response = mock(PlcReadResponse.class);
		if (!missing) {
			when(response.getResponseCode("bad")).thenReturn(java.util.Arrays.stream(
					org.apache.plc4x.java.api.types.PlcResponseCode.values()).filter(code ->
					code != org.apache.plc4x.java.api.types.PlcResponseCode.OK).findFirst().orElseThrow());
		}
		prepareRequests(CompletableFuture.completedFuture(response), new CompletableFuture<>());
		final com.yunyoucloud.plc4x.core.PlcParseData data = new com.yunyoucloud.plc4x.core.PlcParseData();
		data.getRequestItem().setTagName("bad");
		data.getRequestItem().setAddress("address");
		final PLC plc = new PLC(config, manager);
		assertThrows(com.yunyoucloud.plc4x.exception.PlcReadExpection.class,
				() -> plc.read(List.of(data), null));
		assertTrue(plc.isConnected());
		plc.close();
	}
}
