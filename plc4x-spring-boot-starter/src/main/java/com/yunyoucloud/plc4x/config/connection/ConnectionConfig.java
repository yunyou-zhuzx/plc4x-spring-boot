package com.yunyoucloud.plc4x.config.connection;

import com.yunyoucloud.plc4x.core.enums.PlcProtocol;

public interface ConnectionConfig {

	/**
	 * 是否启用
	 *
	 * @return 是否启用
	 */
	boolean enable();

	/**
	 * 连接地址
	 *
	 * @return 连接地址
	 */
	String address();

	/**
	 * 协议
	 *
	 * @return 协议
	 */
	PlcProtocol protocol();

	/**
	 * 单次读写等待超时，单位为毫秒。
	 */
	default long requestTimeoutMillis() {
		return 10000;
	}

	/**
	 * 失败后的重连冷却时间，单位为毫秒。
	 */
	default long reconnectIntervalMillis() {
		return 5000;
	}
}
