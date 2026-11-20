package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.PlcParseData;
import com.yunyoucloud.plc4x.core.annotations.PlcVariable;
import com.yunyoucloud.plc4x.core.enums.EDataType;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import com.yunyoucloud.plc4x.serializer.IPLCSerializable;
import com.yunyoucloud.plc4x.serializer.OpcuaSerializable;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.messages.PlcWriteRequest;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.metadata.PlcConnectionMetadata;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PlcWriteResultTest {
	/**
	 * 仅模拟驱动响应，不连接现场设备。
	 */
	private PlcConnection connection;
	private ConnectionConfig config;
	private DefaultPlcDriverManager manager;
	private PlcWriteRequest request;
	private PlcWriteResponse response;

	@BeforeEach
	void setUp() throws Exception {
		connection = mock(PlcConnection.class);
		config = mock(ConnectionConfig.class);
		manager = mock(DefaultPlcDriverManager.class);
		final PlcDriver driver = mock(PlcDriver.class);
		when(config.address()).thenReturn("opcua:tcp://localhost:4840");
		when(config.protocol()).thenReturn(PlcProtocol.OPCUA);
		when(config.requestTimeoutMillis()).thenReturn(5000L);
		when(config.connectTimeoutMillis()).thenReturn(5000L);
		when(config.closeTimeoutMillis()).thenReturn(5000L);
		when(config.reconnectIntervalMillis()).thenReturn(5000L);
		when(manager.getDriverForUrl(anyString())).thenReturn(driver);
		when(driver.getConnection(anyString())).thenReturn(connection);
		final PlcConnectionMetadata metadata = mock(PlcConnectionMetadata.class);
		when(connection.getMetadata()).thenReturn(metadata);
		when(metadata.isWriteSupported()).thenReturn(true);
		final PlcWriteRequest.Builder builder = mock(PlcWriteRequest.Builder.class);
		request = mock(PlcWriteRequest.class);
		response = mock(PlcWriteResponse.class);
		when(connection.writeRequestBuilder()).thenReturn(builder);
		when(builder.build()).thenReturn(request);
		doReturn(CompletableFuture.completedFuture(response)).when(request).execute();
	}

	/**
	 * 所有协议状态中只有 OK 可以被报告为成功。
	 */
	@ParameterizedTest
	@EnumSource(PlcResponseCode.class)
	void singleWriteRequiresOk(final PlcResponseCode code) throws Exception {
		when(response.getResponseCode("tag")).thenReturn(code);
		final PLC plc = new PLC(config, manager);
		assertEquals(code == PlcResponseCode.OK, plc.write("tag", "address", 1));
		verify(connection, never()).close();
	}

	/**
	 * 批量请求中每一个期望字段都必须有成功状态，缺失字段不能漏检。
	 */
	@Test
	void batchWriteRequiresEveryExpectedTag() {
		final PLC plc = new PLC(config, manager);
		final List<PlcParseData> data = List.of(item("first"), item("second"));
		when(response.getResponseCode("first")).thenReturn(PlcResponseCode.OK);
		assertFalse(plc.write(data));
		when(response.getResponseCode("second")).thenReturn(PlcResponseCode.ACCESS_DENIED);
		assertFalse(plc.write(data));
		when(response.getResponseCode("second")).thenReturn(PlcResponseCode.OK);
		assertTrue(plc.write(data));
	}

	/**
	 * 无写入字段以及空响应都不能报告成功。
	 */
	@Test
	void emptyBatchAndNullResponseAreNotSuccessful() {
		final PLC plc = new PLC(config, manager);
		assertFalse(plc.write(List.of()));
		verify(request, never()).execute();
		doReturn(CompletableFuture.completedFuture(null)).when(request).execute();
		assertFalse(plc.write("address", 1));
	}

	/**
	 * 重复字段名会覆盖驱动请求，必须在发送前拒绝。
	 */
	@Test
	void duplicateTagsAreRejectedBeforeSending() {
		final PLC plc = new PLC(config, manager);
		assertThrows(IllegalArgumentException.class, () -> plc.write(List.of(item("tag"), item("tag"))));
		verify(request, never()).execute();
	}

	/**
	 * 请求已提交但响应未返回时，不得提前报告成功。
	 */
	@Test
	void writeWaitsForResponse() throws Exception {
		final CompletableFuture<PlcWriteResponse> pending = new CompletableFuture<>();
		final CountDownLatch submitted = new CountDownLatch(1);
		doAnswer(invocation -> {
			submitted.countDown();
			return pending;
		}).when(request).execute();
		when(response.getResponseCode("tag")).thenReturn(PlcResponseCode.OK);
		final PLC plc = new PLC(config, manager);
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(1));
		try {
			final Future<Boolean> result = executor.submit(() -> plc.write("tag", "address", 1));
			assertTrue(submitted.await(2, TimeUnit.SECONDS));
			assertFalse(result.isDone());
			pending.complete(response);
			assertTrue(result.get(2, TimeUnit.SECONDS));
		} finally {
			pending.cancel(true);
			executor.shutdownNow();
		}
	}

	/**
	 * 超时代表结果未确认，取消本地等待但保留尚未确认断开的连接。
	 */
	@Test
	void timeoutDoesNotReportSuccessAndKeepsConnection() throws Exception {
		when(config.requestTimeoutMillis()).thenReturn(1L);
		final CompletableFuture<PlcWriteResponse> pending = new CompletableFuture<>();
		doReturn(pending).when(request).execute();
		final PLC plc = new PLC(config, manager);
		final RuntimeException failure = assertThrows(RuntimeException.class, () -> plc.write("address", 1));
		assertInstanceOf(TimeoutException.class, failure.getCause());
		assertTrue(pending.isCancelled());
		verify(connection, never()).close();
		assertTrue(plc.isConnected());
	}

	/**
	 * 明确断链时保留原有异常包装并释放连接，不能降级为写入成功。
	 */
	@Test
	void exceptionalResponsePreservesFailureAndClosesConnection() throws Exception {
		doReturn(CompletableFuture.failedFuture(new java.nio.channels.ClosedChannelException())).when(request).execute();
		final PLC plc = new PLC(config, manager);
		final RuntimeException failure = assertThrows(RuntimeException.class, () -> plc.write("address", 1));
		assertInstanceOf(ExecutionException.class, failure.getCause());
		verify(connection).close();
	}

	/**
	 * 类型快捷方法必须向上传递结果，包括成功与失败。
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void typedMethodsPropagateResult(final boolean success) {
		when(response.getResponseCode("tmpTag")).thenReturn(success ? PlcResponseCode.OK : PlcResponseCode.REMOTE_ERROR);
		final PLC plc = new PLC(config, manager);
		assertEquals(success, plc.writeBoolean("address", true));
		assertEquals(success, plc.writeByte("address", (byte) 1));
		assertEquals(success, plc.writeShort("address", (short) 1));
		assertEquals(success, plc.writeInteger("address", 1));
		assertEquals(success, plc.writeLong("address", 1L));
		assertEquals(success, plc.writeFloat("address", 1F));
		assertEquals(success, plc.writeDouble("address", 1D));
		assertEquals(success, plc.writeString("address", "value"));
	}

	/**
	 * 序列化接口的单点和对象写入不能丢弃底层的返回结果。
	 */
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	void serializerPropagatesResult(final boolean success) {
		final PLC plc = mock(PLC.class);
		when(plc.write(anyString(), any())).thenReturn(success);
		when(plc.write(anyList())).thenReturn(success);
		final IPLCSerializable serializer = new OpcuaSerializable(plc);
		assertEquals(success, serializer.writeBoolean("address", true));
		assertEquals(success, serializer.writeInteger("address", 1));
		assertEquals(success, serializer.write(new WriteBean()));
		assertEquals(success, serializer.write(new WriteBean(), null));
	}

	/**
	 * 构造批量写入字段。
	 */
	private PlcParseData item(final String tagName) {
		final PlcParseData item = new PlcParseData();
		item.getRequestItem().setTagName(tagName);
		item.getRequestItem().setAddress("address");
		item.getResponseItem().setValue(1);
		return item;
	}

	/**
	 * 用于验证对象序列化写入的字段。
	 */
	public static class WriteBean {
		/**
		 * 设备整数节点。
		 */
		@PlcVariable(address = "ns=2;i=1", type = EDataType.INT32)
		public Integer value = 1;
	}
}
