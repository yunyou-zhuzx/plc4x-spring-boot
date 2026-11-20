package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.opcua.config.OpcuaConfiguration;
import org.apache.plc4x.java.opcua.context.Conversation;
import org.apache.plc4x.java.opcua.context.OpcuaDriverContext;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.opcua.security.SecurityPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@Timeout(10)
class ManagedOpcuaSecureChannelTest {
	@Test
	void stoppingCancelsPendingRenewalAndLateResponseCannotRestartIt() throws Exception {
		final Conversation conversation = mock(Conversation.class);
		when(conversation.getSecurityPolicy()).thenReturn(SecurityPolicy.NONE);
		final OpcuaDriverContext context = mock(OpcuaDriverContext.class);
		when(context.getEndpoint()).thenReturn("opc.tcp://localhost:4840");
		final OpcuaConfiguration configuration = mock(OpcuaConfiguration.class);
		when(configuration.getNegotiationTimeout()).thenReturn(2000L);
		final ManagedRequestTransactionManager transactions = new ManagedRequestTransactionManager();
		final ManagedOpcuaSecureChannel channel = spy(new ManagedOpcuaSecureChannel(conversation, transactions, context, configuration, null));
		final CompletableFuture<OpenSecureChannelResponse> delayed = new CompletableFuture<>();
		doReturn(delayed).when(channel).onConnectOpenSecureChannel(eq(SecurityTokenRequestType.securityTokenRequestTypeRenew), anyInt(), anyInt());
		try {
			ReflectionTestUtils.setField(channel, "lifetime", 20L);
			ReflectionTestUtils.invokeMethod(channel, "scheduleRenewal");
			verify(channel, timeout(1000)).onConnectOpenSecureChannel(eq(SecurityTokenRequestType.securityTokenRequestTypeRenew), anyInt(), anyInt());
			assertEquals(1, transactions.pendingCount());
			channel.stopLocal();
			assertEquals(0, transactions.pendingCount());
			delayed.complete(mock(OpenSecureChannelResponse.class));
			ReflectionTestUtils.invokeMethod(channel, "scheduleRenewal");
			assertNull(ReflectionTestUtils.getField(channel, "renewal"));
			assertTrue(Thread.getAllStackTraces().keySet().stream()
					.filter(thread -> thread.getName().equals("plc-opcua-renew")).allMatch(Thread::isDaemon));
		} finally {
			channel.stopLocal();
			transactions.shutdown();
		}
	}
}
