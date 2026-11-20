package org.apache.plc4x.java.opcua.context;

import org.apache.plc4x.java.opcua.readwrite.MessagePDU;
import org.apache.plc4x.java.opcua.protocol.chunk.Chunk;
import org.apache.plc4x.java.opcua.security.SecurityPolicy;
import org.apache.plc4x.java.spi.generation.WriteBufferByteBased;
import org.springframework.util.ReflectionUtils;

import java.security.PrivateKey;
import java.security.Signature;
import java.lang.reflect.Field;
import java.util.Objects;

/**
 * 适配包级加密入口；每个入站报文只解密一次，并修正非对称验签判断。
 */
public final class ManagedOpcuaEncryption extends EncryptionHandler {
	private MessagePDU input;
	private MessagePDU decoded;

	public ManagedOpcuaEncryption(final Conversation conversation, final PrivateKey key) {
		super(conversation, key);
		final Field field = ReflectionUtils.findField(EncryptionHandler.class, "asymmetricEncryptionHandler");
		if (Objects.isNull(field)) {
			throw new IllegalStateException("Unsupported PLC4X encryption implementation");
		}
		ReflectionUtils.makeAccessible(field);
		ReflectionUtils.setField(field, this, new VerifiedAsymmetric(conversation, key));
	}

	public MessagePDU inspect(final MessagePDU message) {
		input = message;
		decoded = super.decodeMessage(message);
		return decoded;
	}

	@Override
	public MessagePDU decodeMessage(final MessagePDU message) {
		// 只缓存同一分片对象，不能按报文内容相等来复用解密结果。
		return message == input && Objects.nonNull(decoded) ? decoded : super.decodeMessage(message);
	}

	public void clear() {
		input = null;
		decoded = null;
	}

	/**
	 * 使用与上游相同的算法和证书，仅在验签失败时拒绝报文。
	 */
	public static final class VerifiedAsymmetric extends AsymmetricEncryptionHandler {
		private final Conversation conversation;
		private final SecurityPolicy policy;

		public VerifiedAsymmetric(final Conversation conversation, final PrivateKey key) {
			super(conversation, conversation.getSecurityPolicy(), key);
			this.conversation = conversation;
			policy = conversation.getSecurityPolicy();
		}

		@Override
		protected void verify(final WriteBufferByteBased buffer, final Chunk chunk, final int messageLength) throws Exception {
			int plainLength = messageLength;
			if (chunk.isEncrypted()) {
				final int blocks = (messageLength - 12 - chunk.getSecurityHeaderSize()) / chunk.getCipherTextBlockSize();
				plainLength -= blocks * (chunk.getCipherTextBlockSize() - chunk.getPlainTextBlockSize());
			}
			final int signatureStart = plainLength - chunk.getSignatureSize();
			final Signature signature = policy.getAsymmetricSignatureAlgorithm().getSignature();
			signature.initVerify(conversation.getRemoteCertificate().getPublicKey());
			signature.update(buffer.getBytes(0, signatureStart));
			if (!signature.verify(buffer.getBytes(signatureStart, plainLength))) {
				throw new IllegalArgumentException("Invalid OPC UA signature");
			}
		}
	}
}
