package com.yunyoucloud.plc4x.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.spi.connection.ChannelExposingConnection;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 连接关闭失败时执行传输层兜底，随后让驱动完成协议和通道工厂清理。
 */
@Slf4j
final class PlcConnectionCleaner {
	/**
	 * 传输层关闭等待上限；驱动自身的关闭等待由 PLC4X 控制。
	 */
	private static final long CHANNEL_CLOSE_TIMEOUT_SECONDS = 2;

	private PlcConnectionCleaner() {
	}

	/**
	 * 最多调用两次驱动关闭；接口声明抛出 Exception，因此在此边界聚合关闭异常。
	 */
	static void close(final PlcConnection connection) throws Exception {
		boolean interrupted = Thread.interrupted();
		try {
			final Channel channel = connection instanceof ChannelExposingConnection exposing
					? exposing.getChannel() : null;
			if (Objects.nonNull(channel) && channel.eventLoop().inEventLoop()) {
				channel.close();
				throw new PlcConnectionException("PLC cleanup must be completed outside the channel event loop");
			}
			try {
				connection.close();
				return;
			} catch (Exception firstFailure) {
				interrupted |= Thread.interrupted() || firstFailure instanceof InterruptedException;
				log.warn("PLC 正常关闭失败，执行通道关闭并重试驱动清理", firstFailure);
				final PlcConnectionException cleanupFailure = new PlcConnectionException("PLC connection cleanup failed", firstFailure);
				try {
					closeChannel(channel);
				} catch (Exception transportFailure) {
					addSuppressed(cleanupFailure, transportFailure);
				}
				try {
					connection.close();
				} catch (Exception retryFailure) {
					interrupted |= Thread.interrupted() || retryFailure instanceof InterruptedException;
					if (!Objects.equals(firstFailure, retryFailure)) {
						addSuppressed(cleanupFailure, retryFailure);
					}
					throw cleanupFailure;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * 关闭通道以触发 PLC4X 注册的关闭回调，不直接终止未知归属的事件循环组。
	 */
	private static void closeChannel(final Channel channel) throws PlcConnectionException {
		if (Objects.isNull(channel)) {
			return;
		}
		final ChannelFuture future = channel.close();
		if (!future.awaitUninterruptibly(CHANNEL_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
			throw new PlcConnectionException("Timed out closing PLC transport channel");
		}
		if (!future.isSuccess()) {
			throw new PlcConnectionException("Failed to close PLC transport channel", future.cause());
		}
	}

	/**
	 * 部分驱动会重复抛出同一个异常对象，避免向自身添加异常。
	 */
	private static void addSuppressed(final Exception failure, final Exception suppressed) {
		if (!Objects.equals(failure, suppressed)) {
			failure.addSuppressed(suppressed);
		}
	}
}