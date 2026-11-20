package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.exceptions.PlcProtocolException;
import org.apache.plc4x.java.opcua.context.*;
import org.apache.plc4x.java.opcua.protocol.chunk.Chunk;
import org.apache.plc4x.java.opcua.security.SecurityPolicy;
import org.apache.plc4x.java.spi.generation.WriteBufferByteBased;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import java.security.*;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(15)
class ManagedOpcuaSecurityTest {
	@Test
	void encryptedAsymmetricSignatureUsesDecryptedLength() throws Exception {
		final SecurityPolicy policy = Arrays.stream(SecurityPolicy.values())
				.filter(value -> value.getSecurityPolicyUri().endsWith("#Basic256Sha256")).findFirst().orElseThrow();
		final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(2048);
		final KeyPair keys = generator.generateKeyPair();
		final X509Certificate certificate = mock(X509Certificate.class);
		when(certificate.getPublicKey()).thenReturn(keys.getPublic());
		final Conversation conversation = mock(Conversation.class);
		when(conversation.getSecurityPolicy()).thenReturn(policy);
		when(conversation.getRemoteCertificate()).thenReturn(certificate);
		final byte[] body = new byte[204];
		new java.security.SecureRandom().nextBytes(body);
		final Signature signing = policy.getAsymmetricSignatureAlgorithm().getSignature();
		signing.initSign(keys.getPrivate());
		signing.update(body);
		final byte[] signature = signing.sign();
		final WriteBufferByteBased buffer = new WriteBufferByteBased(544);
		buffer.writeByteArray("body", body);
		buffer.writeByteArray("signature", signature);
		final Chunk chunk = mock(Chunk.class);
		when(chunk.isEncrypted()).thenReturn(true);
		when(chunk.getSecurityHeaderSize()).thenReturn(20);
		when(chunk.getCipherTextBlockSize()).thenReturn(256);
		when(chunk.getPlainTextBlockSize()).thenReturn(214);
		when(chunk.getSignatureSize()).thenReturn(signature.length);
		final var managed = new ManagedOpcuaEncryption.VerifiedAsymmetric(conversation, keys.getPrivate());
		assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(managed, "verify", buffer, chunk, 544));
	}

	@Test
	void sequenceValidationRejectsReplayAndGapsAndAcceptsRollover() throws Exception {
		assertEquals(5, ManagedOpcuaConversation.validateSequence(4, 5));
		assertThrows(PlcProtocolException.class, () -> ManagedOpcuaConversation.validateSequence(4, 4));
		assertThrows(PlcProtocolException.class, () -> ManagedOpcuaConversation.validateSequence(4, 6));
		assertThrows(PlcProtocolException.class, () -> ManagedOpcuaConversation.validateSequence(4, 3));
		assertEquals(1, ManagedOpcuaConversation.validateSequence(4294966272L, 1));
		assertEquals(0, ManagedOpcuaConversation.validateSequence(4294967295L, 0));
	}

	@Test
	void validSignatureIsAcceptedAndTamperedSignatureIsRejected() throws Exception {
		final SecurityPolicy policy = Arrays.stream(SecurityPolicy.values())
				.filter(value -> value.getSecurityPolicyUri().endsWith("#Basic256Sha256")).findFirst().orElseThrow();
		final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(2048);
		final KeyPair keys = generator.generateKeyPair();
		final X509Certificate certificate = mock(X509Certificate.class);
		when(certificate.getPublicKey()).thenReturn(keys.getPublic());
		final Conversation conversation = mock(Conversation.class);
		when(conversation.getSecurityPolicy()).thenReturn(policy);
		when(conversation.getRemoteCertificate()).thenReturn(certificate);
		final byte[] body = "OPC UA signature regression".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		final Signature signing = policy.getAsymmetricSignatureAlgorithm().getSignature();
		signing.initSign(keys.getPrivate());
		signing.update(body);
		final byte[] signature = signing.sign();
		final WriteBufferByteBased buffer = new WriteBufferByteBased(body.length + signature.length);
		buffer.writeByteArray("body", body);
		buffer.writeByteArray("signature", signature);
		final Chunk chunk = mock(Chunk.class);
		when(chunk.getSignatureSize()).thenReturn(signature.length);
		final var nativeHandler = new AsymmetricEncryptionHandler(conversation, policy, keys.getPrivate());
		final var managed = new ManagedOpcuaEncryption.VerifiedAsymmetric(conversation, keys.getPrivate());
		assertThrows(IllegalArgumentException.class, () -> ReflectionTestUtils.invokeMethod(nativeHandler, "verify", buffer, chunk, body.length + signature.length));
		assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(managed, "verify", buffer, chunk, body.length + signature.length));
		body[0] ^= 1;
		final WriteBufferByteBased tampered = new WriteBufferByteBased(body.length + signature.length);
		tampered.writeByteArray("body", body);
		tampered.writeByteArray("signature", signature);
		assertThrows(IllegalArgumentException.class, () -> ReflectionTestUtils.invokeMethod(managed, "verify", tampered, chunk, body.length + signature.length));
	}
}
