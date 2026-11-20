package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.exceptions.PlcProtocolException;
import org.apache.plc4x.java.opcua.config.OpcuaConfiguration;
import org.apache.plc4x.java.opcua.context.*;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.opcua.security.SecurityPolicy;
import org.apache.plc4x.java.spi.ConversationContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;

/**
 * 在关联请求之前验证所有入站分片，超时请求的晚到响应也必须推进序列状态。
 */
final class ManagedOpcuaConversation extends Conversation {
	private final ManagedOpcuaEncryption encryption;
	private long lastSequence = -1;
	private SequenceHeader current;

	ManagedOpcuaConversation(final ConversationContext<OpcuaAPU> context, final OpcuaDriverContext driver,
							 final OpcuaConfiguration configuration) {
		super(context, driver, configuration);
		encryption = new ManagedOpcuaEncryption(this, getSecurityPolicy() == SecurityPolicy.NONE ? null : driver.getCertificateKeyPair().getPrivateKey());
		setPrivate("encryptionHandler", encryption);
		final BiPredicate<SequenceHeader, CompletableFuture<?>> validator = (header, future) -> {
			if (Objects.nonNull(current) && header.getSequenceNumber() == current.getSequenceNumber()
					&& header.getRequestId() == current.getRequestId()) {
				return true;
			}
			future.completeExceptionally(new PlcProtocolException("OPC UA response bypassed sequence validation"));
			return false;
		};
		setPrivate("sequenceValidator", validator);
	}

	private void setPrivate(final String name, final Object value) {
		final Field field = ReflectionUtils.findField(Conversation.class, name);
		if (Objects.isNull(field)) {
			throw new IllegalStateException("Unsupported PLC4X conversation: " + name);
		}
		ReflectionUtils.makeAccessible(field);
		ReflectionUtils.setField(field, this, value);
	}

	void receive(final OpcuaAPU packet, final Runnable dispatch) throws PlcProtocolException {
		try {
			final MessagePDU message = packet.getMessage();
			if (message instanceof OpcuaOpenResponse || message instanceof OpcuaMessageResponse) {
				final MessagePDU decoded = encryption.inspect(message);
				final SequenceHeader header = decoded instanceof OpcuaOpenResponse open
						? open.getMessage().getSequenceHeader() : ((OpcuaMessageResponse) decoded).getMessage().getSequenceHeader();
				acceptSequence(header.getSequenceNumber());
				current = header;
			}
			dispatch.run();
		} finally {
			current = null;
			encryption.clear();
		}
	}

	/**
	 * 按 UInt32 处理序号，支持传统安全策略规定的回绕区间，拒绝重复和跳号。
	 */
	void acceptSequence(final int number) throws PlcProtocolException {
		lastSequence = validateSequence(lastSequence, number);
	}

	static long validateSequence(final long lastSequence, final int number) throws PlcProtocolException {
		final long sequence = Integer.toUnsignedLong(number);
		final boolean rollover = lastSequence > 4294966271L && sequence < 1024;
		if (lastSequence >= 0 && sequence != lastSequence + 1 && !rollover) {
			throw new PlcProtocolException("Invalid OPC UA sequence: previous=" + lastSequence + ", received=" + sequence);
		}
		return sequence;
	}
}
