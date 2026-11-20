package com.yunyoucloud.plc4x.client;

import cn.hutool.core.collection.CollUtil;
import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.PlcParseData;
import com.yunyoucloud.plc4x.core.RequestItem;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import com.yunyoucloud.plc4x.exception.PlcReadExpection;
import com.yunyoucloud.plc4x.exception.PlcWriteExpection;
import com.yunyoucloud.plc4x.resolve.PlcResolve;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.messages.PlcWriteRequest;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.spi.connection.ChannelExposingConnection;

import java.io.EOFException;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

@Slf4j
@Getter
@SuppressWarnings("unchecked")
public class PLC implements AutoCloseable {

	/**
	 * 同一应用类加载器中的客户端共享驱动注册表，各设备连接仍独立管理。
	 */
	private static final DefaultPlcDriverManager SHARED_DRIVER_MANAGER = new DefaultPlcDriverManager();

	/**
	 * 对同一连接的阻塞生命周期任务提供超时和实际完成状态跟踪。
	 */
	@Getter(AccessLevel.NONE)
	private final PlcLifecycleExecutor lifecycleExecutor = new PlcLifecycleExecutor();

	/**
	 * 当前连接的所有权由本对象管理，所有替换和关闭均使用同一把锁。
	 */
	private volatile PlcConnection plcConnection;
	/**
	 * 保留最近可见的真实通道，驱动自行关闭并清空引用后仍可识别断链。
	 */
	@Getter(AccessLevel.NONE)
	private Channel transportChannel;
	private final PlcProtocol plcProtocol;
	private final ConnectionConfig connectionConfig;
	private final DefaultPlcDriverManager driverManager;
	/**
	 * 最后一次资源清理失败，供显式关闭向调用方报告。
	 */
	private Exception closeFailure;
	private boolean connected;
	private volatile boolean closed;
	private long nextReconnectNanos;

	private static final Set<Class<?>> WRAPPER_TYPES = new HashSet<>(Set.of(
			Integer.class, Boolean.class, Byte.class, Short.class,
			Long.class, Float.class, Double.class, Character.class
	));

	public PLC(final ConnectionConfig connectionConfig) {
		this(connectionConfig, SHARED_DRIVER_MANAGER, true);
	}

	/**
	 * 支持注入驱动管理器以验证异常路径的资源释放。
	 */
	PLC(final ConnectionConfig connectionConfig, final DefaultPlcDriverManager driverManager) {
		this(connectionConfig, driverManager, true);
	}

	/**
	 * 延迟连接时先交付对象所有权，初次握手失败仍可由调用方继续清理。
	 */
	public PLC(final ConnectionConfig connectionConfig, final boolean connectImmediately) {
		this(connectionConfig, SHARED_DRIVER_MANAGER, connectImmediately);
	}

	@SneakyThrows
	PLC(
			final ConnectionConfig connectionConfig,
			final DefaultPlcDriverManager driverManager,
			final boolean connectImmediately
	) {
		this.connectionConfig = Objects.requireNonNull(connectionConfig);
		this.plcProtocol = connectionConfig.protocol();
		this.driverManager = Objects.requireNonNull(driverManager);
		if (connectionConfig.connectTimeoutMillis() <= 0 || connectionConfig.closeTimeoutMillis() <= 0
				|| connectionConfig.requestTimeoutMillis() <= 0 || connectionConfig.reconnectIntervalMillis() < 0) {
			throw new IllegalArgumentException("PLC timeouts must be positive and reconnect interval must not be negative");
		}
		if (connectImmediately) {
			try {
				createNewConnection();
			} catch (PlcConnectionException | RuntimeException failure) {
				if (Objects.nonNull(plcConnection) || lifecycleExecutor.hasPendingOperation()) {
					final PlcInitializationException recoverable = new PlcInitializationException(this, failure);
					if (Objects.nonNull(closeFailure) && !Objects.equals(closeFailure, failure)) {
						recoverable.addSuppressed(closeFailure);
					}
					throw recoverable;
				}
				throw failure;
			}
		}
	}

	/**
	 * 根据本地生命周期状态复用连接；实际通信异常负责触发失效。
	 */
	public synchronized boolean checkAndReconnectConnection() {
		if (closed) {
			return false;
		}
		if (Objects.nonNull(plcConnection) && connected && !isTransportDisconnected()) {
			return true;
		}
		return reconnectWithRetry();
	}

	/**
	 * 每个冷却周期仅尝试一次，避免持锁休眠和密集重建网络资源。
	 */
	public synchronized boolean reconnectWithRetry() {
		if (closed) {
			return false;
		}
		if (Objects.nonNull(plcConnection) && connected && !isTransportDisconnected()) {
			return true;
		}
		if (System.nanoTime() - nextReconnectNanos < 0) {
			return false;
		}
		try {
			createNewConnection();
			return true;
		} catch (PlcConnectionException e) {
			log.warn("PLC 重连失败，进入冷却期: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * 无论是否在线都必须关闭；关闭失败时保留引用，禁止覆盖未释放的资源。
	 */
	public synchronized void destroyStaleConnection() {
		connected = false;
		nextReconnectNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(connectionConfig.reconnectIntervalMillis());
		if (lifecycleExecutor.hasPendingOperation()) {
			closeFailure = new PlcConnectionException("PLC lifecycle operation is still running; retain client and retry close()");
			return;
		}
		if (Objects.nonNull(plcConnection)) {
			if (closeConnection(plcConnection)) {
				plcConnection = null;
				transportChannel = null;
			}
		} else {
			closeFailure = null;
		}
	}

	/**
	 * 永久关闭客户端，后续读写不得自动重连；重复调用可重试失败的清理。
	 */
	@Override
	public synchronized void close() {
		closed = true;
		destroyStaleConnection();
		if (Objects.nonNull(plcConnection) || lifecycleExecutor.hasPendingOperation()) {
			throw new IllegalStateException("PLC connection cleanup failed; retry close() before discarding this client", closeFailure);
		}
	}

	/**
	 * 创建并接管连接，先持有对象再连接，保证握手失败时仍能释放资源。
	 */
	public synchronized void createNewConnection() throws PlcConnectionException {
		if (closed) {
			throw new PlcConnectionException("PLC client is closed");
		}
		destroyStaleConnection();
		if (Objects.nonNull(plcConnection) || lifecycleExecutor.hasPendingOperation()) {
			throw new PlcConnectionException("Previous PLC connection could not be closed", closeFailure);
		}
		boolean success = false;
		try {
			final String address = connectionConfig.address();
			connectOwnedConnection(address);
			if (plcConnection instanceof ChannelExposingConnection exposing) {
				final Channel channel = exposing.getChannel();
				transportChannel = channel instanceof EmbeddedChannel ? null : channel;
			}
			connected = true;
			nextReconnectNanos = System.nanoTime();
			success = true;
		} finally {
			if (!success) {
				destroyStaleConnection();
			}
		}
	}

	/**
	 * 驱动接口及执行器会报告不同检查异常，在建连边界统一转交原始原因。
	 */
	private void connectOwnedConnection(final String address) throws PlcConnectionException {
		try {
			lifecycleExecutor.run(() -> {
				final PlcDriver driver = ManagedDrivers.select(driverManager.getDriverForUrl(address));
				final PlcConnection connection = Objects.requireNonNull(driver.getConnection(address), "PLC driver connection");
				plcConnection = connection;
				if (closed || lifecycleExecutor.isCancellationRequested()) {
					throw new PlcConnectionException("PLC client closed or timed out during connection creation");
				}
				connection.connect();
			}, connectionConfig.connectTimeoutMillis());
		} catch (PlcConnectionException failure) {
			throw failure;
		} catch (Exception failure) {
			throw new PlcConnectionException("PLC connect failed", failure);
		}
	}

	/**
	 * 连接接口声明抛出 Exception，清理边界必须捕获并记录，不能掩盖原始故障。
	 */
	private boolean closeConnection(final PlcConnection connection) {
		final boolean interrupted = Thread.interrupted();
		try {
			lifecycleExecutor.run(() -> PlcConnectionCleaner.close(connection), connectionConfig.closeTimeoutMillis());
			closeFailure = null;
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			closeFailure = e;
			log.warn("PLC 连接清理被中断，已尝试传输层兜底", e);
			return false;
		} catch (Exception e) {
			closeFailure = e;
			log.error("PLC 通道兜底及关闭重试后仍未完成清理，禁止替换连接", e);
			return false;
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * 查询本地生命周期状态，不代表远端设备当前可达。
	 */
	public synchronized boolean isConnected() {
		return !closed && connected && Objects.nonNull(plcConnection) && !isTransportDisconnected();
	}

	/**
	 * 仅使用真实传输通道判断断链，不依赖驱动的在线标志或 S7H 虚拟通道。
	 */
	private boolean isTransportDisconnected() {
		if (plcConnection instanceof ChannelExposingConnection exposing) {
			final Channel channel = exposing.getChannel();
			if (channel instanceof EmbeddedChannel) {
				return false;
			}
			if (Objects.nonNull(channel)) {
				transportChannel = channel;
			}
		}
		return Objects.nonNull(transportChannel) && (!transportChannel.isOpen() || !transportChannel.isActive());
	}

	/**
	 * 仅明确的传输断链异常触发清理，协议错误和普通请求异常不代表连接关闭。
	 */
	private boolean isDisconnectionFailure(final Throwable failure) {
		final Set<Throwable> visited = new HashSet<>();
		Throwable cause = failure;
		while (Objects.nonNull(cause) && visited.add(cause)) {
			if (cause instanceof ClosedChannelException || cause instanceof EOFException
					|| cause instanceof SocketException) {
				return true;
			}
			cause = cause.getCause();
		}
		return false;
	}

	/**
	 * 所有读写统一限制等待时间；请求失败时取消等待，仅确认断链才清理连接。
	 */
	private <T> T awaitResponse(final Supplier<CompletableFuture<T>> request) throws ExecutionException, InterruptedException {
		CompletableFuture<T> response = null;
		boolean success = false;
		boolean disconnected = false;
		try {
			final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(connectionConfig.requestTimeoutMillis());
			response = PlcRequestBudget.submit(deadline, request);
			final long remaining = deadline - System.nanoTime();
			if (remaining <= 0) {
				throw new TimeoutException("PLC request budget exhausted during submission");
			}
			final T result = response.get(remaining, TimeUnit.NANOSECONDS);
			success = true;
			return result;
		} catch (TimeoutException failure) {
			throw new IllegalStateException("PLC request timed out", failure);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		} catch (ExecutionException | RuntimeException e) {
			// 提交和异步完成都可能包装传输异常，在统一边界分类后原样抛出。
			disconnected = isDisconnectionFailure(e);
			throw e;
		} finally {
			if (!success) {
				if (Objects.nonNull(response)) {
					response.cancel(true);
				}
				if (disconnected || isTransportDisconnected()) {
					destroyStaleConnection();
				}
			}
		}
	}

	/**
	 * 写入保留原有运行时异常包装，异常原因链仍可识别协议错误和明确断链。
	 */
	private <T extends PlcWriteResponse> T awaitWriteResponse(final Supplier<CompletableFuture<T>> request) {
		try {
			return awaitResponse(request);
		} catch (ExecutionException | InterruptedException failure) {
			throw new IllegalStateException("PLC write request failed", failure);
		}
	}

	public boolean readBoolean(String address) {
		return read(address, Boolean.class);
	}

	public byte readByte(String address) {
		return read(address, Byte.class);
	}

	public short readShort(String address) {
		return read(address, Short.class);
	}

	public int readInteger(String address) {
		return read(address, Integer.class);
	}

	public long readLong(String address) {
		return read(address, Long.class);
	}

	public float readFloat(String address) {
		return read(address, Float.class);
	}

	public double readDouble(String address) {
		return read(address, Double.class);
	}

	public String readString(String address) {
		return read(address, String.class);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeBoolean(final String address, final boolean value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeByte(final String address, final byte value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeShort(final String address, final short value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeInteger(final String address, final int value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeLong(final String address, final long value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeFloat(final String address, final float value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeDouble(final String address, final double value) {
		return write(address, value);
	}

	/**
	 * 写入单值并返回协议确认结果，通信异常时抛出异常。
	 */
	public boolean writeString(final String address, final String value) {
		return write(address, value);
	}

	/**
	 * 写入前检查本地连接生命周期及驱动能力。
	 */
	private void ensureReadSupported() throws PlcConnectionException {
		if (!checkAndReconnectConnection()) {
			throw new PlcConnectionException("Unable to establish PLC connection");
		}
		if (!plcConnection.getMetadata().isReadSupported()) {
			throw new PlcReadExpection("This connection doesn't support read.");
		}
	}

	public <T> T read(final String address, final Class<T> returnClass) {
		return read("tmpTag", address, returnClass);
	}

	@SneakyThrows
	public synchronized <T> T read(final String tagName, final String address, final Class<T> returnClass) {
		ensureReadSupported();

		final PlcReadRequest.Builder requestBuilder = this.plcConnection.readRequestBuilder();
		requestBuilder.addTagAddress(tagName, address);
		final PlcReadRequest plcReadRequest = requestBuilder.build();
		final PlcReadResponse plcReadResponse = awaitResponse(plcReadRequest::execute);
		if (Objects.isNull(plcReadResponse)) {
			throw new PlcReadExpection("PLC read response is missing");
		}
		final PlcResponseCode responseCode = plcReadResponse.getResponseCode(tagName);
		if (responseCode == PlcResponseCode.OK) {
			return resolvePlcValue(tagName, plcReadResponse, returnClass);
		} else {
			throw new PlcReadExpection("PLC field read failed: " + tagName + ", status=" + responseCode);
		}
	}

	public void read(final PlcParseData plcParseData) {
		final Object value = read(
				plcParseData.getRequestItem().getTagName(),
				plcParseData.getRequestItem().getAddress(),
				plcParseData.getDataType().getClazz()
		);

		plcParseData.getResponseItem().setValue(value);
	}

	@SneakyThrows
	public synchronized void read(final List<PlcParseData> plcParseDataList, final PlcResolve plcResolve) {
		if (CollUtil.isEmpty(plcParseDataList)) {
			return;
		}
		final Set<String> requestedTags = new HashSet<>(plcParseDataList.size());
		for (final PlcParseData data : plcParseDataList) {
			final String name = Objects.requireNonNull(data.getRequestItem().getTagName(), "tagName");
			if (!requestedTags.add(name)) {
				throw new IllegalArgumentException("Duplicate read tag: " + name);
			}
		}
		ensureReadSupported();

		final PlcReadRequest.Builder requestBuilder = this.plcConnection.readRequestBuilder();

		final Map<String, PlcParseData> plcParseDataMap = new HashMap<>(8);
		for (PlcParseData plcParseData : plcParseDataList) {
			final RequestItem requestItem = plcParseData.getRequestItem();
			plcParseDataMap.put(requestItem.getTagName(), plcParseData);
			requestBuilder.addTagAddress(requestItem.getTagName(), requestItem.getAddress());
		}

		final PlcReadRequest plcReadRequest = requestBuilder.build();
		final PlcReadResponse plcReadResponse = awaitResponse(plcReadRequest::execute);
		if (Objects.isNull(plcReadResponse)) {
			throw new PlcReadExpection("PLC read response is missing");
		}

		// 必要字段必须全部有效，禁止将部分空值组装为正常设备状态。
		for (String requestedTag : plcParseDataMap.keySet()) {
			if (plcReadResponse.getResponseCode(requestedTag) != PlcResponseCode.OK) {
				throw new PlcReadExpection("PLC field read failed: " + requestedTag);
			}
		}
		final Map<String, Object> values = new HashMap<>(plcParseDataMap.size());
		for (final Map.Entry<String, PlcParseData> item : plcParseDataMap.entrySet()) {
			values.put(item.getKey(), plcResolve.resolve(item.getValue(), item.getKey(), plcReadResponse));
		}
		// 所有字段解析完成后再更新样本，避免解析异常留下半份新数据。
		for (final Map.Entry<String, PlcParseData> item : plcParseDataMap.entrySet()) {
			item.getValue().getResponseItem().setValue(values.get(item.getKey()));
		}
	}

	/**
	 * 写入前检查本地连接生命周期及驱动能力。
	 */
	private void ensureWriteSupported() throws PlcConnectionException {
		if (!checkAndReconnectConnection()) {
			throw new PlcConnectionException("Unable to establish PLC connection");
		}
		if (!plcConnection.getMetadata().isWriteSupported()) {
			throw new PlcWriteExpection("This connection doesn't support writing.");
		}
	}

	/**
	 * 写入单值并等待字段的写响应。
	 */
	public <T> boolean write(final String address, final T value) {
		return write("tmpTag", address, value);
	}

	/**
	 * 等待写响应，仅当字段返回 OK 时返回 true；通信异常表示写入结果未确认。
	 */
	@SneakyThrows
	public synchronized <T> boolean write(final String tagName, final String address, final T value) {
		ensureWriteSupported();
		final PlcWriteRequest.Builder builder = plcConnection.writeRequestBuilder();
		builder.addTagAddress(tagName, address, value);
		final PlcWriteRequest request = builder.build();
		return isWriteSuccessful(awaitWriteResponse(request::execute), List.of(tagName));
	}

	/**
	 * 批量写入所有字段均返回 OK 才成功；空请求返回 false，部分成功不会回滚。
	 */
	@SneakyThrows
	public synchronized boolean write(final List<PlcParseData> plcParseDataList) {
		if (CollUtil.isEmpty(plcParseDataList)) {
			return false;
		}
		final Set<String> tagNames = new HashSet<>(plcParseDataList.size());
		for (PlcParseData data : plcParseDataList) {
			final String tagName = Objects.requireNonNull(data.getRequestItem().getTagName(), "tagName");
			if (!tagNames.add(tagName)) {
				throw new IllegalArgumentException("Duplicate write tag: " + tagName);
			}
		}
		ensureWriteSupported();
		final PlcWriteRequest.Builder builder = plcConnection.writeRequestBuilder();
		for (PlcParseData data : plcParseDataList) {
			final RequestItem item = data.getRequestItem();
			builder.addTagAddress(item.getTagName(), item.getAddress(), data.getResponseItem().getValue());
		}
		final PlcWriteRequest request = builder.build();
		return isWriteSuccessful(awaitWriteResponse(request::execute), tagNames);
	}

	/**
	 * 按请求字段检查结果，避免空响应、缺失状态或部分成功被误报为成功。
	 */
	private boolean isWriteSuccessful(final PlcWriteResponse response, final Collection<String> tagNames) {
		if (Objects.isNull(response)) {
			log.warn("PLC 写响应为空，无法确认写入结果");
			return false;
		}
		boolean success = true;
		for (String tagName : tagNames) {
			final PlcResponseCode code = response.getResponseCode(tagName);
			if (code != PlcResponseCode.OK) {
				log.warn("PLC 写入未确认成功: tagName = {}, responseCode = {}", tagName, code);
				success = false;
			}
		}
		return success;
	}

	public <T> T resolvePlcValue(
			final String tagName,
			final PlcReadResponse plcReadResponse,
			final Class<T> returnClass
	) {
		if (WRAPPER_TYPES.contains(returnClass)) {
			if (returnClass == Boolean.class) {
				return (T) plcReadResponse.getBoolean(tagName);
			}
			if (returnClass == Byte.class) {
				return (T) plcReadResponse.getByte(tagName);
			}
			if (returnClass == Character.class) {
				return (T) Character.valueOf((char) Byte.toUnsignedInt(plcReadResponse.getByte(tagName)));
			}
			if (returnClass == Short.class) {
				return (T) plcReadResponse.getShort(tagName);
			}
			if (returnClass == Integer.class) {
				return (T) plcReadResponse.getInteger(tagName);
			}
			if (returnClass == Long.class) {
				return (T) plcReadResponse.getLong(tagName);
			}
			if (returnClass == Float.class) {
				return (T) plcReadResponse.getFloat(tagName);
			}
			if (returnClass == Double.class) {
				return (T) plcReadResponse.getDouble(tagName);
			}
		}
		return resolveCustomValue(tagName, plcReadResponse, returnClass);
	}

	private <T> T resolveCustomValue(
			final String tagName,
			final PlcReadResponse plcReadResponse,
			final Class<T> returnClass
	) {
		if (returnClass == String.class) {
			return (T) plcReadResponse.getString(tagName);
		}
		return null;
	}
}
