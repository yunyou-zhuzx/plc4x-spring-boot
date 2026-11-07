package com.yunyoucloud.plc4x.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.spi.connection.ChannelExposingConnection;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PlcConnectionCleanerTest {
	/**
	 * 正常关闭成功时无需再触碰传输层。
	 */
	@Test
	void successfulCloseDoesNotCloseChannelAgain() throws Exception {
		final PlcConnection connection = mock(PlcConnection.class);
		PlcConnectionCleaner.close(connection);
		verify(connection).close();
	}

	/**
	 * 驱动首次关闭异常后，应先关闭通道，再重试驱动清理。
	 */
	@Test
	void failedCloseClosesTransportBeforeRetry() throws Exception {
		final PlcConnection connection = nettyConnection();
		final Channel channel = mock(Channel.class);
		when(((ChannelExposingConnection) connection).getChannel()).thenReturn(channel);
		when(channel.eventLoop()).thenReturn(mock(EventLoop.class));
		final ChannelFuture future = mock(ChannelFuture.class);
		when(channel.close()).thenReturn(future);
		when(future.awaitUninterruptibly(2, TimeUnit.SECONDS)).thenReturn(true);
		when(future.isSuccess()).thenReturn(true);
		doThrow(new PlcConnectionException("first close failed")).doNothing().when(connection).close();

		PlcConnectionCleaner.close(connection);

		final InOrder order = inOrder(connection, channel);
		order.verify(connection).close();
		order.verify(channel).close();
		order.verify(connection).close();
		verify(future).awaitUninterruptibly(2, TimeUnit.SECONDS);
	}

	/**
	 * 通道关闭超时不能阻止驱动关闭重试，失败信息需要一起保留。
	 */
	@Test
	void transportTimeoutAndRetryFailureAreReported() throws Exception {
		final PlcConnection connection = nettyConnection();
		final Channel channel = mock(Channel.class);
		when(((ChannelExposingConnection) connection).getChannel()).thenReturn(channel);
		when(channel.eventLoop()).thenReturn(mock(EventLoop.class));
		final ChannelFuture future = mock(ChannelFuture.class);
		when(channel.close()).thenReturn(future);
		final PlcConnectionException failure = new PlcConnectionException("close failed");
		doThrow(failure).when(connection).close();

		final PlcConnectionException cleanupFailure = assertThrows(PlcConnectionException.class, () -> PlcConnectionCleaner.close(connection));
		assertSame(failure, cleanupFailure.getCause());
		assertEquals(1, cleanupFailure.getSuppressed().length);
		assertEquals(0, failure.getSuppressed().length);
		verify(connection, times(2)).close();
		verify(channel).close();
	}

	/**
	 * 中断标记不能阻止兜底重试，退出时必须恢复。
	 */
	@Test
	void interruptionIsClearedDuringCleanupAndRestoredAfterwards() throws Exception {
		final PlcConnection connection = mock(PlcConnection.class);
		doThrow(new InterruptedException("interrupted")).doAnswer(invocation -> {
			assertFalse(Thread.currentThread().isInterrupted());
			return null;
		}).when(connection).close();
		Thread.currentThread().interrupt();
		try {
			PlcConnectionCleaner.close(connection);
			assertTrue(Thread.currentThread().isInterrupted());
			verify(connection, times(2)).close();
		} finally {
			Thread.interrupted();
		}
	}

	/**
	 * 真实 Netty 通道关闭后执行与 PLC4X 相同的关闭回调，验证工作线程实际退出。
	 */
	@Test
	void fallbackClosesRealChannelAndTerminatesItsWorker() throws Exception {
		final NioEventLoopGroup group = new NioEventLoopGroup(1);
		final NioSocketChannel channel = new NioSocketChannel();
		try {
			group.register(channel).sync();
			channel.closeFuture().addListener(future -> group.shutdownGracefully(0, 2, TimeUnit.SECONDS));
			final PlcConnection connection = nettyConnection();
			when(((ChannelExposingConnection) connection).getChannel()).thenReturn(channel);
			doThrow(new PlcConnectionException("driver close failed")).when(connection).close();

			assertThrows(PlcConnectionException.class, () -> PlcConnectionCleaner.close(connection));

			assertFalse(channel.isOpen());
			assertTrue(group.terminationFuture().await(5, TimeUnit.SECONDS));
			assertTrue(group.isTerminated());
			verify(connection, times(2)).close();
		} finally {
			channel.close();
			group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(5000);
		}
	}

	/**
	 * 避免在通道自身的事件循环线程中阻塞等待关闭。
	 */
	@Test
	void eventLoopThreadOnlyInitiatesAsynchronousTransportClose() throws Exception {
		final PlcConnection connection = nettyConnection();
		final Channel channel = mock(Channel.class);
		final EventLoop eventLoop = mock(EventLoop.class);
		when(((ChannelExposingConnection) connection).getChannel()).thenReturn(channel);
		when(channel.eventLoop()).thenReturn(eventLoop);
		when(eventLoop.inEventLoop()).thenReturn(true);

		assertThrows(PlcConnectionException.class, () -> PlcConnectionCleaner.close(connection));

		verify(channel).close();
		verify(connection, never()).close();
	}

	/**
	 * 创建能暴露 Netty 通道的驱动连接测试对象。
	 */
	private PlcConnection nettyConnection() {
		return mock(PlcConnection.class, withSettings().extraInterfaces(ChannelExposingConnection.class));
	}
}