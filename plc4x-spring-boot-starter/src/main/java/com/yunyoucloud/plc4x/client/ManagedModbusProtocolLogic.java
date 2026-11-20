package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.modbus.readwrite.ModbusTcpADU;
import org.apache.plc4x.java.modbus.tcp.config.ModbusTcpConfiguration;
import org.apache.plc4x.java.modbus.tcp.protocol.ModbusTcpProtocolLogic;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.configuration.HasConfiguration;

import java.util.concurrent.CompletableFuture;

/**
 * 保留 Modbus 报文实现，修正超时、错误和取消时事务槽不释放的问题。
 */
public class ManagedModbusProtocolLogic extends ModbusTcpProtocolLogic implements HasConfiguration<ModbusTcpConfiguration> {

	private final ManagedRequestTransactionManager transactions = new ManagedRequestTransactionManager();

	@Override
	public void setConfiguration(final ModbusTcpConfiguration configuration) {
		if (configuration.getRequestTimeout() <= 0) {
			throw new IllegalArgumentException("Modbus request timeout must be positive");
		}
		super.setConfiguration(configuration);
		tm.shutdown();
		tm = transactions;
	}

	@Override
	public CompletableFuture<PlcReadResponse> read(final PlcReadRequest request) {
		return transactions.track(() -> super.read(request), requestTimeout.toMillis());
	}

	@Override
	public CompletableFuture<PlcWriteResponse> write(final PlcWriteRequest request) {
		return transactions.track(() -> super.write(request), requestTimeout.toMillis());
	}

	@Override
	public CompletableFuture<PlcPingResponse> ping(final PlcPingRequest request) {
		return transactions.track(() -> super.ping(request), requestTimeout.toMillis());
	}

	@Override
	public void onDisconnect(final ConversationContext<ModbusTcpADU> context) {
		transactions.shutdown();
	}

	@Override
	public void close(final ConversationContext<ModbusTcpADU> context) {
		transactions.shutdown();
	}
}
