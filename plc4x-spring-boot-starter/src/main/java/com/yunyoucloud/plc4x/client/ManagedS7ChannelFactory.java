package com.yunyoucloud.plc4x.client;

import java.net.SocketAddress;

/**
 * S7H 使用通用受管 TCP 工厂，保留专属类型供内部重连及兼容检查使用。
 */
final class ManagedS7ChannelFactory extends ManagedTcpChannelFactory {
	ManagedS7ChannelFactory(final SocketAddress address) {
		super(address);
	}
}
