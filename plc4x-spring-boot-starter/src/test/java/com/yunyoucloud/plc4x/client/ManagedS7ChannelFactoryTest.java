package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.config.connection.S7ConnectionConfig;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.spi.connection.NettyChannelFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 使用真实本地 TCP 验证关闭后引用和专属线程均被回收。
 */
@Timeout(20)
class ManagedS7ChannelFactoryTest {

	@Test
	void repeatedPhysicalDisconnectsReleaseChannelsAndWorkers() throws Exception {
		try (final ServerSocket listener = listener()) {
			final ManagedS7ChannelFactory factory = new ManagedS7ChannelFactory(listener.getLocalSocketAddress());
			try {
				for (int index = 0; index < 20; index++) {
					final Channel channel = factory.createChannel(new ChannelInboundHandlerAdapter());
					final EventLoopGroup group = channel.eventLoop().parent();
					try (final Socket peer = listener.accept()) {
						assertEquals(1, factory.trackedChannelCount());
					}
					assertTrue(channel.closeFuture().await(3, TimeUnit.SECONDS));
					assertTrue(group.terminationFuture().await(3, TimeUnit.SECONDS));
					assertEquals(0, factory.trackedChannelCount());
				}
				// 确认也没有把已关闭通道遗留在上游的私有集合中。
				final Field retained = NettyChannelFactory.class.getDeclaredField("eventLoops");
				retained.setAccessible(true);
				assertEquals(0, ((Map<?, ?>) retained.get(factory)).size());
			} finally {
				factory.closeOwnedChannels();
			}
		}
	}

	@Test
	void refusedConnectionDoesNotRemainTracked() throws Exception {
		final InetSocketAddress address;
		try (final ServerSocket listener = listener()) {
			address = (InetSocketAddress) listener.getLocalSocketAddress();
		}
		final ManagedS7ChannelFactory factory = new ManagedS7ChannelFactory(address);
		try {
			assertThrows(PlcConnectionException.class, () -> factory.createChannel(new ChannelInboundHandlerAdapter()));
			assertEquals(0, factory.trackedChannelCount());
		} finally {
			factory.closeOwnedChannels();
		}
	}

	@Test
	void disposedFactoryRejectsNewReconnects() throws Exception {
		try (final ServerSocket listener = listener()) {
			final ManagedS7ChannelFactory factory = new ManagedS7ChannelFactory(listener.getLocalSocketAddress());
			factory.closeOwnedChannels();
			assertThrows(PlcConnectionException.class, () -> factory.createChannel(new ChannelInboundHandlerAdapter()));
			assertEquals(0, factory.trackedChannelCount());
		}
	}

	@Test
	void refusedS7ConnectionClosesBeforeSupervisorWasScheduled() throws Exception {
		final int port;
		try (final ServerSocket listener = listener()) {
			port = listener.getLocalPort();
		}
		final S7ConnectionConfig config = new S7ConnectionConfig();
		config.setIp("127.0.0.1");
		config.setPort(port);
		config.setControllerType("S7_1200");
		try (final PLC plc = new PLC(config, false)) {
			assertFalse(plc.checkAndReconnectConnection());
			assertNull(plc.getPlcConnection());
			assertFalse(plc.isConnected());
		}
	}

	@Test
	void configuredS7PortIsIncludedInConnectionAddress() {
		final S7ConnectionConfig config = new S7ConnectionConfig();
		config.setIp("127.0.0.1");
		config.setPort(1102);
		assertTrue(config.address().startsWith("s7://127.0.0.1:1102?"));
		config.setPort(102);
		assertTrue(config.address().startsWith("s7://127.0.0.1:102?"));
	}

	@Test
	void dualEndpointConnectionCanCloseBeforeConnect() throws Exception {
		final ManagedS7Driver driver = new ManagedS7Driver();
		try (final PlcConnection connection = driver.getConnection(
				"s7:tcp://127.0.0.1:1102/127.0.0.1:1103?controller-type=S7_1200")) {
			assertNotNull(connection);
		}
	}

	@Test
	void realS7HandshakeTimeoutClosesPhysicalSocket() throws Exception {
		try (final ServerSocket listener = listener()) {
			final ConnectionConfig config = new ConnectionConfig() {
				@Override
				public boolean enable() {
					return true;
				}

				@Override
				public String address() {
					return "s7://127.0.0.1:" + listener.getLocalPort() + "?controller-type=S7_1200";
				}

				@Override
				public PlcProtocol protocol() {
					return PlcProtocol.S7;
				}

				@Override
				public long connectTimeoutMillis() {
					return 500;
				}
			};
			final PLC plc = new PLC(config, false);
			try {
				assertTimeout(Duration.ofSeconds(3), () -> assertFalse(plc.checkAndReconnectConnection()));
				try (final Socket peer = listener.accept()) {
					peer.setSoTimeout(3000);
					retryClose(plc);
					// 服务端没有响应握手；消费已发出的 COTP 报文后必须读到 EOF。
					assertTrue(peer.getInputStream().readAllBytes().length > 0);
				}
				assertNull(plc.getPlcConnection());
			} finally {
				retryClose(plc);
			}
		}
	}

	/**
	 * 测试监听仅限本机随机端口，不访问外部 PLC。
	 */
	private ServerSocket listener() throws Exception {
		final ServerSocket listener = new ServerSocket();
		listener.bind(new InetSocketAddress("127.0.0.1", 0));
		listener.setSoTimeout(3000);
		return listener;
	}

	/**
	 * 中断送达后允许驱动先退出，再关闭同一所有者。
	 */
	private void retryClose(final PLC plc) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
		while (true) {
			try {
				plc.close();
				return;
			} catch (IllegalStateException failure) {
				if (System.nanoTime() - deadline >= 0) {
					throw failure;
				}
				TimeUnit.MILLISECONDS.sleep(5);
			}
		}
	}
}
