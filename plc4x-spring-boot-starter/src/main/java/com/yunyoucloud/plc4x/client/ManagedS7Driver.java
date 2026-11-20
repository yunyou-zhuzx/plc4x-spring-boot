package com.yunyoucloud.plc4x.client;

import cn.hutool.core.util.StrUtil;
import io.netty.channel.Channel;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.s7.readwrite.S7Driver;
import org.apache.plc4x.java.s7.readwrite.TPKTPacket;
import org.apache.plc4x.java.s7.readwrite.context.S7DriverContext;
import org.apache.plc4x.java.s7.readwrite.protocol.S7HSingleProtocolStackConfigurer;
import org.apache.plc4x.java.spi.connection.ProtocolStackConfigurer;
import org.apache.plc4x.java.s7.readwrite.configuration.S7TcpTransportConfiguration;
import org.apache.plc4x.java.s7.readwrite.protocol.S7HPlcConnection;
import org.apache.plc4x.java.spi.configuration.ConfigurationFactory;
import org.apache.plc4x.java.spi.configuration.PlcConnectionConfiguration;
import org.apache.plc4x.java.spi.events.DisconnectEvent;
import org.apache.plc4x.java.transport.tcp.TcpTransportConfiguration;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 适配 PLC4X 0.13.1 的 S7 连接，沿用协议栈并替换存在引用滞留的 TCP 工厂。
 */
final class ManagedS7Driver extends S7Driver {

	@Override
	protected org.apache.plc4x.java.spi.optimizer.BaseOptimizer getOptimizer() {
		return new ManagedS7Optimizer();
	}

	/**
	 * 保留普通 TCP、显式 TCP 及双端点 S7H 地址形式。
	 */
	private static final Pattern CONNECTION = Pattern.compile("^s7(?::tcp)?://([^?]+)(?:\\?(.*))?$");

	@Override
	protected ProtocolStackConfigurer<TPKTPacket> getStackConfigurer() {
		final ProtocolStackConfigurer<TPKTPacket> delegate = S7HSingleProtocolStackConfigurer.builder(TPKTPacket.class, TPKTPacket::staticParse)
				.withProtocol(ManagedS7ProtocolLogic.class)
				.withDriverContext(S7DriverContext.class)
				.withPacketSizeEstimator(ByteLengthEstimator.class)
				.withCorruptPacketRemover(CorruptPackageCleaner.class)
				.build();
		return (configuration, pipeline, authentication, passive, listeners) -> {
			final var protocol = delegate.configurePipeline(configuration, pipeline, authentication, passive, listeners);
			ManagedProtocolResources.install(pipeline);
			return protocol;
		};
	}

	@Override
	public PlcConnection getConnection(final String address) throws PlcConnectionException {
		final Matcher matcher = CONNECTION.matcher(address);
		if (!matcher.matches()) {
			throw new PlcConnectionException("Unsupported S7 TCP connection address");
		}
		final String[] endpoints = matcher.group(1).split("/", -1);
		if (endpoints.length > 2 || StrUtil.isBlank(endpoints[0])
				|| endpoints.length == 2 && StrUtil.isBlank(endpoints[1])) {
			throw new PlcConnectionException("S7 requires one or two TCP endpoints");
		}
		final ConfigurationFactory configurations = new ConfigurationFactory();
		final String parameters = matcher.group(2);
		final PlcConnectionConfiguration configuration = configurations.createConfiguration(
				getConfigurationClass(), "s7", "tcp", endpoints[0], parameters);
		final TcpTransportConfiguration tcp = (TcpTransportConfiguration) configurations.createTransportConfiguration(
				S7TcpTransportConfiguration.class, "s7", "tcp", endpoints[0], parameters);
		final ManagedS7ChannelFactory primary = createFactory(endpoints[0], tcp);
		final ManagedS7ChannelFactory secondary = endpoints.length == 2 ? createFactory(endpoints[1], tcp) : null;
		return new ManagedConnection(configuration, primary, secondary);
	}

	/**
	 * 使用未解析地址，让 DNS 和 TCP 等待都处于受限的连接操作中。
	 */
	private ManagedS7ChannelFactory createFactory(final String endpoint, final TcpTransportConfiguration configuration)
			throws PlcConnectionException {
		try {
			final URI uri = new URI("tcp://" + endpoint);
			final int port = uri.getPort() == -1 ? ISO_ON_TCP_PORT : uri.getPort();
			if (StrUtil.isBlank(uri.getHost()) || port < 1 || port > 65535 || StrUtil.isNotEmpty(uri.getRawUserInfo())) {
				throw new PlcConnectionException("Invalid S7 TCP endpoint");
			}
			final ManagedS7ChannelFactory factory = new ManagedS7ChannelFactory(
					InetSocketAddress.createUnresolved(uri.getHost(), port));
			factory.setConfiguration(configuration);
			return factory;
		} catch (URISyntaxException failure) {
			throw new PlcConnectionException("Invalid S7 TCP endpoint", failure);
		}
	}

	/**
	 * 保留驱动原有的系统属性覆盖行为。
	 */
	private boolean configured(final String property, final boolean fallback) {
		return Boolean.parseBoolean(System.getProperty(property, Boolean.toString(fallback)));
	}

	/**
	 * 初次 TCP 失败尚未启动 S7 监督任务时，也能完成关闭而不触发上游空指针。
	 */
	private final class ManagedConnection extends S7HPlcConnection {

		/**
		 * 本连接拥有的两个通道工厂。
		 */
		private final ManagedS7ChannelFactory primary;
		private final ManagedS7ChannelFactory secondary;

		/**
		 * 上游在发送连接事件之前创建监督任务，记录该阶段以选择安全关闭路径。
		 */
		private volatile boolean supervisionStarted;

		/**
		 * 对未进入上游监督阶段的连接也提供幂等关闭。
		 */
		private boolean disposed;

		private ManagedConnection(final PlcConnectionConfiguration configuration, final ManagedS7ChannelFactory primary,
								  final ManagedS7ChannelFactory secondary) {
			super(ManagedS7Driver.this.canPing(), ManagedS7Driver.this.canRead(), ManagedS7Driver.this.canWrite(),
					ManagedS7Driver.this.canSubscribe(), ManagedS7Driver.this.canBrowse(), ManagedS7Driver.this.getValueHandler(),
					configuration, primary, secondary, false,
					configured(PROPERTY_PLC4X_FORCE_AWAIT_SETUP_COMPLETE, awaitSetupComplete()),
					configured(PROPERTY_PLC4X_FORCE_AWAIT_DISCONNECT_COMPLETE, awaitDisconnectComplete()),
					configured(PROPERTY_PLC4X_FORCE_AWAIT_DISCOVER_COMPLETE, awaitDiscoverComplete()),
					ManagedS7Driver.this.getStackConfigurer(), ManagedS7Driver.this.getOptimizer(), ManagedS7Driver.this.getAuthentication());
			this.primary = primary;
			this.secondary = secondary;
		}

		@Override
		protected void sendChannelCreatedEvent() {
			supervisionStarted = true;
			super.sendChannelCreatedEvent();
		}

		@Override
		public synchronized void close() throws PlcConnectionException {
			if (disposed) {
				return;
			}
			primary.stopAccepting();
			if (Objects.nonNull(secondary)) {
				secondary.stopAccepting();
			}
			try {
				if (supervisionStarted) {
					super.close();
				} else {
					final Channel current = getChannel();
					if (Objects.nonNull(current)) {
						current.pipeline().fireUserEventTriggered(new DisconnectEvent());
					}
					connected = false;
				}
			} finally {
				primary.closeOwnedChannels();
				if (Objects.nonNull(secondary)) {
					secondary.closeOwnedChannels();
				}
			}
			disposed = true;
		}
	}
}
