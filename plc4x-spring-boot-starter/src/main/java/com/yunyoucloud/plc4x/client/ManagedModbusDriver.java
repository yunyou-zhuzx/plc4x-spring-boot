package com.yunyoucloud.plc4x.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.plc4x.java.modbus.readwrite.DriverType;
import org.apache.plc4x.java.modbus.readwrite.ModbusTcpADU;
import org.apache.plc4x.java.modbus.tcp.ModbusTcpDriver;
import org.apache.plc4x.java.modbus.tcp.context.ModbusTcpContext;
import org.apache.plc4x.java.spi.connection.ProtocolStackConfigurer;
import org.apache.plc4x.java.spi.connection.SingleProtocolStackConfigurer;

/**
 * 将项目支持的 Modbus TCP 接入受管事务实现。
 */
final class ManagedModbusDriver extends ModbusTcpDriver {

	@Override
	public org.apache.plc4x.java.api.PlcConnection getConnection(final String address,
																 final org.apache.plc4x.java.api.authentication.PlcAuthentication authentication)
			throws org.apache.plc4x.java.api.exceptions.PlcConnectionException {
		return ManagedTcpChannelFactory.adapt(super.getConnection(address, authentication));
	}

	@Override
	protected ProtocolStackConfigurer<ModbusTcpADU> getStackConfigurer() {
		final ProtocolStackConfigurer<ModbusTcpADU> delegate = SingleProtocolStackConfigurer.builder(ModbusTcpADU.class,
						input -> (ModbusTcpADU) ModbusTcpADU.staticParse(input, DriverType.MODBUS_TCP, true))
				.withProtocol(ManagedModbusProtocolLogic.class)
				.withDriverContext(ModbusTcpContext.class)
				.withPacketSizeEstimator(ByteLengthEstimator.class)
				.build();
		return (configuration, pipeline, authentication, passive, listeners) -> {
			final var protocol = delegate.configurePipeline(configuration, pipeline, authentication, passive, listeners);
			ManagedProtocolResources.install(pipeline);
			pipeline.addLast(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelInactive(final ChannelHandlerContext context) throws Exception {
					((ManagedModbusProtocolLogic) protocol).onDisconnect(null);
					super.channelInactive(context);
				}
			});
			return protocol;
		};
	}
}
