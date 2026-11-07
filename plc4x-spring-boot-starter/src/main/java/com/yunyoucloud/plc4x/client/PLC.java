package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.PlcParseData;
import com.yunyoucloud.plc4x.core.RequestItem;
import com.yunyoucloud.plc4x.core.ResponseItem;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import com.yunyoucloud.plc4x.exception.PlcReadExpection;
import com.yunyoucloud.plc4x.exception.PlcWriteExpection;
import com.yunyoucloud.plc4x.resolve.PlcResolve;
import com.yunyoucloud.plc4x.utils.ByteUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.messages.PlcWriteRequest;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Getter
@SuppressWarnings("unchecked")
public class PLC implements AutoCloseable {

	/**
	 * 当前连接的所有权由本对象管理，所有替换和关闭均使用同一把锁。
	 */
	private PlcConnection plcConnection;
	private final PlcProtocol plcProtocol;
	private final ConnectionConfig connectionConfig;
	private final DefaultPlcDriverManager driverManager;
	private boolean connected;
	private boolean closed;
	private long nextReconnectNanos;

	private static final Set<Class<?>> WRAPPER_TYPES = new HashSet<>(Set.of(
			Integer.class, Boolean.class, Byte.class, Short.class,
			Long.class, Float.class, Double.class, Character.class
	));

	public PLC(final ConnectionConfig connectionConfig) {
		this(connectionConfig, new DefaultPlcDriverManager());
	}

	/**
	 * 支持注入驱动管理器以验证异常路径的资源释放。
	 */
	@SneakyThrows
	PLC(final ConnectionConfig connectionConfig, final DefaultPlcDriverManager driverManager) {
		this.connectionConfig = Objects.requireNonNull(connectionConfig);
		this.plcProtocol = connectionConfig.protocol();
		this.driverManager = Objects.requireNonNull(driverManager);
		createNewConnection();
	}

	/**
	 * 根据本地生命周期状态复用连接；实际通信异常负责触发失效。
	 */
	public synchronized boolean checkAndReconnectConnection() {
		if (closed) {
			return false;
		}
		if (Objects.nonNull(plcConnection) && connected) {
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
		if (Objects.nonNull(plcConnection) && connected) {
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
		if (Objects.nonNull(plcConnection)) {
			if (closeConnection(plcConnection)) {
				plcConnection = null;
			}
		}
	}

	/**
	 * 永久关闭客户端，后续读写不得自动重连；重复调用可重试失败的清理。
	 */
	@Override
	public synchronized void close() {
		closed = true;
		destroyStaleConnection();
	}

	/**
	 * 创建并接管连接，先持有对象再连接，保证握手失败时仍能释放资源。
	 */
	public synchronized PlcConnection createNewConnection() throws PlcConnectionException {
		if (closed) {
			throw new PlcConnectionException("PLC client is closed");
		}
		destroyStaleConnection();
		if (Objects.nonNull(plcConnection)) {
			throw new PlcConnectionException("Previous PLC connection could not be closed");
		}
		boolean success = false;
		try {
			final String address = connectionConfig.address();
			plcConnection = driverManager.getDriverForUrl(address).getConnection(address);
			plcConnection.connect();
			connected = true;
			nextReconnectNanos = System.nanoTime();
			success = true;
			return plcConnection;
		} finally {
			if (!success) {
				destroyStaleConnection();
			}
		}
	}

	/**
	 * 连接接口声明抛出 Exception，清理边界必须捕获并记录，不能掩盖原始故障。
	 */
	private boolean closeConnection(final PlcConnection connection) {
		try {
			connection.close();
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.warn("关闭 PLC 连接被中断", e);
			return false;
		} catch (Exception e) {
			log.warn("关闭 PLC 连接失败，保留引用等待再次清理", e);
			return false;
		}
	}

	/**
	 * 查询本地生命周期状态，不代表远端设备当前可达。
	 */
	public synchronized boolean isConnected() {
		return !closed && connected && Objects.nonNull(plcConnection);
	}

	/**
	 * 所有读写统一限制等待时间；失败时取消请求并清理连接。
	 */
	private <T> T awaitResponse(final Supplier<CompletableFuture<T>> request) throws ExecutionException, TimeoutException, InterruptedException {
		CompletableFuture<T> response = null;
		boolean success = false;
		try {
			response = request.get();
			final T result = response.get(connectionConfig.requestTimeoutMillis(), TimeUnit.MILLISECONDS);
			success = true;
			return result;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		} finally {
			if (!success) {
				if (Objects.nonNull(response)) {
					response.cancel(true);
				}
				// 清理可能消耗中断标记，退出前恢复调用线程的中断状态。
				final boolean interrupted = Thread.currentThread().isInterrupted();
				try {
					destroyStaleConnection();
				} finally {
					if (interrupted) {
						Thread.currentThread().interrupt();
					}
				}
			}
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

	public void writeBoolean(String address, boolean value) {
		write(address, value);
	}

	public void writeByte(String address, byte value) {
		write(address, value);
	}

	public void writeShort(String address, short value) {
		write(address, value);
	}

	public void writeInteger(String address, int value) {
		write(address, value);
	}

	public void writeLong(String address, long value) {
		write(address, value);
	}

	public void writeFloat(String address, float value) {
		write(address, value);
	}

	public void writeDouble(String address, double value) {
		write(address, value);
	}

	public void writeString(String address, String value) {
		write(address, value);
	}

	public <T> T read(final String address, final Class<T> returnClass) {
		return read("tmpTag", address, returnClass);
	}

	@SneakyThrows
	public synchronized <T> T read(final String tagName, final String address, final Class<T> returnClass) {
		if (!checkAndReconnectConnection()) {
			log.error("Unable to establish PLC connection, plc address: {}", connectionConfig.address());
			throw new PlcConnectionException("Unable to establish PLC connection");
		}

		if (!plcConnection.getMetadata().isReadSupported()) {
			log.error("This connection doesn't support reading, plc address: {}", connectionConfig.address());
			throw new PlcReadExpection("This connection doesn't support reading.");
		}

		try {
			final PlcReadRequest.Builder requestBuilder = this.plcConnection.readRequestBuilder();
			requestBuilder.addTagAddress(tagName, address);
			final PlcReadRequest plcReadRequest = requestBuilder.build();
			final PlcReadResponse plcReadResponse = awaitResponse(plcReadRequest::execute);
			final PlcResponseCode responseCode = plcReadResponse.getResponseCode(tagName);
			if (responseCode == PlcResponseCode.OK) {
				return resolvePlcValue(tagName, plcReadResponse, returnClass);
			} else {
				log.warn("read address {} failed, tagName = {}, reason = {}", address, tagName, responseCode.name());
				return null;
			}
		} catch (TimeoutException e) {
			destroyStaleConnection();
			throw new RuntimeException(e);
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
		if (!checkAndReconnectConnection()) {
			log.error("Unable to establish PLC connection, plc address: {}", connectionConfig.address());
			throw new PlcConnectionException("Unable to establish PLC connection");
		}

		if (!plcConnection.getMetadata().isReadSupported()) {
			log.error("This connection doesn't support reading, plc address: {}", connectionConfig.address());
			throw new PlcReadExpection("This connection doesn't support reading.");
		}

		try {
			final PlcReadRequest.Builder requestBuilder = this.plcConnection.readRequestBuilder();

			final Map<String, PlcParseData> plcParseDataMap = new HashMap<>(8);
			for (PlcParseData plcParseData : plcParseDataList) {
				final RequestItem requestItem = plcParseData.getRequestItem();
				plcParseDataMap.put(requestItem.getTagName(), plcParseData);
				requestBuilder.addTagAddress(requestItem.getTagName(), requestItem.getAddress());
			}

			final PlcReadRequest plcReadRequest = requestBuilder.build();
			final PlcReadResponse plcReadResponse = awaitResponse(plcReadRequest::execute);

			for (String tagName : plcReadResponse.getTagNames()) {
				final PlcResponseCode responseCode = plcReadResponse.getResponseCode(tagName);
				final PlcParseData plcParseData = plcParseDataMap.get(tagName);
				if (responseCode == PlcResponseCode.OK) {
					final RequestItem requestItem = plcParseData.getRequestItem();
					final Object value = plcResolve.resolve(plcParseData, requestItem.getTagName(), plcReadResponse);
					plcParseData.getResponseItem().setValue(value);
				} else {
					log.error("读取PLC数据失败：tagName = {}, reason = {}", tagName, responseCode.name());
					plcParseData.getResponseItem().setValue(null);
				}
			}
		} catch (TimeoutException e) {
			destroyStaleConnection();
			throw new RuntimeException(e);
		}
	}

	public <T> void write(final String address, final T value) {
		write("tmpTag", address, value);
	}

	@SneakyThrows
	public synchronized <T> void write(final String tagName, final String address, final T value) {
		if (!checkAndReconnectConnection()) {
			log.error("Unable to establish PLC connection, plc address: {}", connectionConfig.address());
			throw new PlcConnectionException("Unable to establish PLC connection");
		}

		if (!plcConnection.getMetadata().isWriteSupported()) {
			log.error("This connection doesn't support writing, plc address: {}", connectionConfig.address());
			throw new PlcWriteExpection("This connection doesn't support writing.");
		}

		try {
			final PlcWriteRequest.Builder writeRequestBuilder = this.plcConnection.writeRequestBuilder();
			writeRequestBuilder.addTagAddress(tagName, address, value);
			final PlcWriteRequest plcWriteRequest = writeRequestBuilder.build();
			final PlcWriteResponse plcReadResponse = awaitResponse(plcWriteRequest::execute);
			final PlcResponseCode responseCode = plcReadResponse.getResponseCode(tagName);
			if (responseCode != PlcResponseCode.OK) {
				log.warn("write address {} failed, tagName = {}, reason = {}", address, tagName, responseCode.name());
				throw new PlcWriteExpection(String.format("tagName = %s, reason = %s", tagName, responseCode.name()));
			}
		} catch (ExecutionException | TimeoutException | PlcWriteExpection e) {
			destroyStaleConnection();
			throw new RuntimeException(e);
		}
	}

	@SneakyThrows
	public synchronized void write(final List<PlcParseData> plcParseDataList) {
		if (!checkAndReconnectConnection()) {
			log.error("Unable to establish PLC connection, plc address: {}", connectionConfig.address());
			throw new PlcConnectionException("Unable to establish PLC connection");
		}

		if (!plcConnection.getMetadata().isWriteSupported()) {
			log.error("This connection doesn't support writing, plc address: {}", connectionConfig.address());
			throw new PlcWriteExpection("This connection doesn't support writing.");
		}

		try {
			final PlcWriteRequest.Builder writeRequestBuilder = this.plcConnection.writeRequestBuilder();

			for (PlcParseData plcParseData : plcParseDataList) {
				final RequestItem requestItem = plcParseData.getRequestItem();
				final ResponseItem responseItem = plcParseData.getResponseItem();
				writeRequestBuilder.addTagAddress(requestItem.getTagName(), requestItem.getAddress(), responseItem.getValue());
			}

			final PlcWriteRequest plcWriteRequest = writeRequestBuilder.build();
			final PlcWriteResponse plcWriteResponse = awaitResponse(plcWriteRequest::execute);

			for (String tagName : plcWriteResponse.getTagNames()) {
				final PlcResponseCode responseCode = plcWriteResponse.getResponseCode(tagName);
				if (responseCode != PlcResponseCode.OK) {
					log.error("写入PLC数据失败：tagName = {}, reason = {}", tagName, responseCode.name());
				}
			}
		} catch (ExecutionException | TimeoutException | PlcWriteExpection e) {
			destroyStaleConnection();
			throw new RuntimeException(e);
		}
	}

	public <T> T resolvePlcValue(final String tagName, final PlcReadResponse plcReadResponse, final Class<T> returnClass) {
		if (WRAPPER_TYPES.contains(returnClass)) {
			if (returnClass == Boolean.class) {
				return (T) plcReadResponse.getBoolean(tagName);
			}
			if (returnClass == Byte.class) {
				return (T) plcReadResponse.getByte(tagName);
			}
			if (returnClass == Character.class) {
				return (T) plcReadResponse.getByte(tagName);
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

	private <T> T resolveCustomValue(final String tagName, final PlcReadResponse plcReadResponse, final Class<T> returnClass) {
		if (returnClass == String.class) {
			return (T) plcReadResponse.getString(tagName);
		}
		return null;
	}

	public static void main(String[] args) {
		int test = 12;
		final int i = ByteUtils.setBits(test, 3, 1, 0);
		System.out.println(Integer.toBinaryString(i));

		String testStr = "estun.main";
		for (Short stringToShort : ByteUtils.stringToShorts(testStr)) {
			System.out.println(Integer.toString(stringToShort, 16));
		}

		int test2 = 1073741824;
		System.out.println(Float.intBitsToFloat(test2));

//		System.out.println(ByteUtils.shortToString((short) getBits(29541, "[16]", ByteOrder.BIG_ENDIAN)));
	}

}
