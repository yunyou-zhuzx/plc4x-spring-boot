package com.yunyoucloud.plc4x.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.transport.tcp.TcpChannelFactory;

import java.net.SocketAddress;

import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.spi.connection.DefaultNettyPlcConnection;
import org.apache.plc4x.java.spi.connection.NettyChannelFactory;
import org.apache.plc4x.java.transport.tcp.TcpTransportConfiguration;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 管理 TCP 每次建连及重连创建的通道及其专属线程组，关闭后立即移除引用。
 */
class ManagedTcpChannelFactory extends TcpChannelFactory {

	/**
	 * 仅记录本工厂创建且尚未关闭的通道，不使用上游不会自动清除的集合。
	 */
	private final Map<Channel, EventLoopGroup> channels = new ConcurrentHashMap<>();

	/**
	 * 保留目标地址，上游同名字段为私有成员。
	 */
	private final SocketAddress address;

	/**
	 * 关闭开始后拒绝内部重连，防止回收期间又创建网络资源。
	 */
	private volatile boolean closed;

	ManagedTcpChannelFactory(final SocketAddress address) {
		super(address);
		this.address = address;
	}

	/**
	 * 在原始连接交付前替换尚未开始建连的标准 TCP 工厂。
	 */
	static PlcConnection adapt(final PlcConnection connection) {
		final Field owned = requiredField(DefaultNettyPlcConnection.class, "channelFactory");
		final Object original = ReflectionUtils.getField(owned, connection);
		if (original.getClass() == TcpChannelFactory.class) {
			final SocketAddress address = (SocketAddress) ReflectionUtils.getField(requiredField(NettyChannelFactory.class, "remoteAddress"), original);
			final ManagedTcpChannelFactory replacement = new ManagedTcpChannelFactory(address);
			replacement.setConfiguration((TcpTransportConfiguration) ReflectionUtils.getField(requiredField(TcpChannelFactory.class, "configuration"), original));
			ReflectionUtils.setField(owned, connection, replacement);
		}
		return connection;
	}

	private static Field requiredField(final Class<?> type, final String name) {
		final Field field = ReflectionUtils.findField(type, name);
		if (Objects.isNull(field)) {
			throw new IllegalStateException("Unsupported PLC4X TCP implementation: " + name);
		}
		ReflectionUtils.makeAccessible(field);
		return field;
	}

	/**
	 * 真实通道已关闭时不再向已终止的事件循环发送 DisconnectEvent；仍显式清理协议资源。
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	static boolean closeInactive(final PlcConnection connection) {
		if (connection.getClass() != DefaultNettyPlcConnection.class) {
			return false;
		}
		final DefaultNettyPlcConnection nativeConnection = (DefaultNettyPlcConnection) connection;
		final Channel channel = nativeConnection.getChannel();
		final Object factory = ReflectionUtils.getField(requiredField(DefaultNettyPlcConnection.class, "channelFactory"), connection);
		if (!(factory instanceof ManagedTcpChannelFactory managed) || Objects.isNull(channel) || channel.isOpen()) {
			return false;
		}
		final org.apache.plc4x.java.spi.Plc4xProtocolBase protocol = nativeConnection.getProtocol();
		if (Objects.nonNull(protocol)) {
			protocol.close(protocol.getConversationContext());
		}
		managed.closeEventLoopForChannel(channel);
		ReflectionUtils.setField(requiredField(DefaultNettyPlcConnection.class, "channel"), connection, null);
		ReflectionUtils.setField(requiredField(DefaultNettyPlcConnection.class, "connected"), connection, false);
		return true;
	}

	@Override
	public Channel createChannel(final ChannelHandler handler) throws PlcConnectionException {
		if (closed) {
			throw new PlcConnectionException("PLC TCP channel factory is closed");
		}
		final EventLoopGroup group = getEventLoopGroup();
		Channel channel = null;
		boolean delivered = false;
		try {
			final Bootstrap bootstrap = createBootstrap().group(group).channel(getChannel()).handler(handler);
			configureBootstrap(bootstrap);
			final ChannelFuture connected = bootstrap.connect(address);
			channel = connected.channel();
			final Channel ownedChannel = channel;
			channels.put(channel, group);
			channel.closeFuture().addListener(future -> release(ownedChannel, group));
			if (closed) {
				throw new PlcConnectionException("PLC TCP channel factory closed during connect");
			}
			connected.await();
			if (!connected.isSuccess()) {
				throw new PlcConnectionException("PLC TCP connect failed", connected.cause());
			}
			delivered = true;
			return channel;
		} catch (InterruptedException failure) {
			Thread.currentThread().interrupt();
			throw new PlcConnectionException("PLC TCP connect interrupted", failure);
		} catch (RuntimeException failure) {
			throw new PlcConnectionException("PLC TCP connect failed", failure);
		} finally {
			if (!delivered || closed || Thread.currentThread().isInterrupted()) {
				if (Objects.nonNull(channel) && channel.isOpen()) {
					channel.close();
				}
				release(channel, group);
			}
		}
	}

	/**
	 * 拒绝新连接，但让驱动先执行其协议关闭流程。
	 */
	void stopAccepting() {
		closed = true;
	}

	/**
	 * 只关闭本工厂拥有的物理通道，关闭回调负责释放对应线程组。
	 */
	void closeOwnedChannels() {
		stopAccepting();
		channels.forEach((channel, group) -> {
			if (channel.isOpen()) {
				channel.close();
			}
			release(channel, group);
		});
	}

	@Override
	public void closeEventLoopForChannel(final Channel channel) {
		final EventLoopGroup group = channels.remove(channel);
		if (Objects.nonNull(group)) {
			group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
		}
	}

	/**
	 * 提供回归测试所需的资源计数。
	 */
	int trackedChannelCount() {
		return channels.size();
	}

	/**
	 * 回调和异常分支都可重复执行，只处理本次创建的专属线程组。
	 */
	private void release(final Channel channel, final EventLoopGroup group) {
		if (Objects.nonNull(channel)) {
			channels.remove(channel, group);
		}
		group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
	}
}
