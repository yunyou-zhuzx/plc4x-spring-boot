package com.yunyoucloud.plc4x.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.plc4x.java.opcua.OpcuaPlcDriver;
import org.apache.plc4x.java.opcua.context.OpcuaDriverContext;
import org.apache.plc4x.java.opcua.readwrite.OpcuaAPU;
import org.apache.plc4x.java.spi.connection.ProtocolStackConfigurer;
import org.apache.plc4x.java.spi.connection.SingleProtocolStackConfigurer;

/**
 * 保留 OPC UA 编码和安全策略，在远端直接断链时也执行本地资源回收。
 */
final class ManagedOpcuaDriver extends OpcuaPlcDriver {

	@Override
	public org.apache.plc4x.java.api.PlcConnection getConnection(final String address,
																 final org.apache.plc4x.java.api.authentication.PlcAuthentication authentication)
			throws org.apache.plc4x.java.api.exceptions.PlcConnectionException {
		return ManagedTcpChannelFactory.adapt(super.getConnection(address, authentication));
	}

	@Override
	protected ProtocolStackConfigurer<OpcuaAPU> getStackConfigurer() {
		final ProtocolStackConfigurer<OpcuaAPU> delegate = SingleProtocolStackConfigurer
				.builder(OpcuaAPU.class, input -> OpcuaAPU.staticParse(input, true, true))
				.withProtocol(ManagedOpcuaProtocolLogic.class)
				.withPacketSizeEstimator(ByteLengthEstimator.class)
				.withDriverContext(OpcuaDriverContext.class)
				.littleEndian().build();
		return (configuration, pipeline, authentication, passive, listeners) -> {
			final var protocol = delegate.configurePipeline(configuration, pipeline, authentication, passive, listeners);
			ManagedProtocolResources.install(pipeline);
			final String wrapper = pipeline.context(org.apache.plc4x.java.spi.Plc4xNettyWrapper.class).name();
			pipeline.addBefore(wrapper, "managed-opcua-sequence", new ChannelInboundHandlerAdapter() {
				@Override
				public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
					if (message instanceof OpcuaAPU packet) {
						try {
							((ManagedOpcuaProtocolLogic) protocol).receive(packet, () -> context.fireChannelRead(message));
						} catch (org.apache.plc4x.java.api.exceptions.PlcProtocolException | RuntimeException failure) {
							((ManagedOpcuaProtocolLogic) protocol).rejectReceived(failure);
							context.fireExceptionCaught(failure);
						}
					} else {
						context.fireChannelRead(message);
					}
				}
			});
			pipeline.addLast(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelInactive(final ChannelHandlerContext context) throws Exception {
					((ManagedOpcuaProtocolLogic) protocol).transportClosed();
					super.channelInactive(context);
				}
			});
			return protocol;
		};
	}
}
