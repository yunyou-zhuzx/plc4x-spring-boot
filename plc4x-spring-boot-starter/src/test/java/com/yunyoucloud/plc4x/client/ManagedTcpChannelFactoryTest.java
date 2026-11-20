package com.yunyoucloud.plc4x.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.spi.connection.ChannelExposingConnection;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@Timeout(15)
class ManagedTcpChannelFactoryTest {
	@Test
	void interruptedConnectClosesUnpublishedChannelEvenIfSuccessArrivesLate() throws Exception {
		final EmbeddedChannel channel = new EmbeddedChannel();
		final NioEventLoopGroup group = new NioEventLoopGroup(1);
		final DefaultEventExecutor executor = new DefaultEventExecutor();
		final ChannelPromise connected = new DefaultChannelPromise(channel, executor);
		final Bootstrap bootstrap = mock(Bootstrap.class, RETURNS_SELF);
		when(bootstrap.connect(any(SocketAddress.class))).thenReturn(connected);
		final ManagedTcpChannelFactory factory = new ManagedTcpChannelFactory(new InetSocketAddress("127.0.0.1", 102)) {
			@Override
			protected Bootstrap createBootstrap() {
				return bootstrap;
			}

			@Override
			public EventLoopGroup getEventLoopGroup() {
				return group;
			}
		};
		try {
			group.submit(() -> {
			}).sync();
			final AtomicReference<Throwable> failure = new AtomicReference<>();
			final Thread caller = new Thread(() -> {
				Thread.currentThread().interrupt();
				try {
					factory.createChannel(new ChannelInboundHandlerAdapter());
				} catch (org.apache.plc4x.java.api.exceptions.PlcConnectionException error) {
					failure.set(error);
				}
			});
			caller.start();
			caller.join(3000);
			assertFalse(caller.isAlive());
			assertNotNull(failure.get());
			connected.trySuccess();
			assertFalse(channel.isOpen());
			assertTrue(group.terminationFuture().await(3, TimeUnit.SECONDS));
			assertEquals(0, factory.trackedChannelCount());
		} finally {
			factory.closeOwnedChannels();
			channel.finishAndReleaseAll();
			group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
			executor.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
		}
	}

	@Test
	void bothStandardDriversUseManagedTcpFactory() throws Exception {
		try (final PlcConnection modbus = new ManagedModbusDriver().getConnection("modbus-tcp://127.0.0.1:1502");
			 final PlcConnection opcua = new ManagedOpcuaDriver().getConnection("opcua:tcp://127.0.0.1:4840")) {
			assertInstanceOf(ManagedTcpChannelFactory.class, ReflectionTestUtils.getField(modbus, "channelFactory"));
			assertInstanceOf(ManagedTcpChannelFactory.class, ReflectionTestUtils.getField(opcua, "channelFactory"));
		}
	}

	@Test
	void closeAfterEventLoopTerminationStillReleasesConnection() throws Exception {
		try (final ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
			 final PlcConnection connection = new ManagedModbusDriver().getConnection("modbus-tcp://127.0.0.1:" + server.getLocalPort())) {
			connection.connect();
			try (final Socket peer = server.accept()) {
				final Channel channel = ((ChannelExposingConnection) connection).getChannel();
				channel.close().sync();
				assertTrue(channel.eventLoop().parent().terminationFuture().await(3, TimeUnit.SECONDS));
				assertDoesNotThrow(() -> PlcConnectionCleaner.close(connection));
				assertNull(((ChannelExposingConnection) connection).getChannel());
			}
		}
	}
}
