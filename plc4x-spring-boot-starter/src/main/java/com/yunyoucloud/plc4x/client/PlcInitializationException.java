package com.yunyoucloud.plc4x.client;

/**
 * 首次连接失败且资源尚未释放时交付客户端所有权，调用方可继续关闭同一对象。
 */
public final class PlcInitializationException extends IllegalStateException {

	/**
	 * 仍然拥有待清理资源的客户端，不能丢弃后直接创建替代连接。
	 */
	private final PLC client;

	PlcInitializationException(final PLC client, final Throwable cause) {
		super("PLC initialization failed with resources pending cleanup; retain getClient() and retry close()", cause);
		this.client = client;
	}

	/**
	 * 返回可重试关闭的客户端。
	 */
	public PLC getClient() {
		return client;
	}
}
