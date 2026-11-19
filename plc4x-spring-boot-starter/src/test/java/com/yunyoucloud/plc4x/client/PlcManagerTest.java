package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PlcManagerTest {

	@Test
	void clientsShareDriverManagerAndCloseIndependently() {
		final ConnectionConfig config = connectionConfig();
		try (final PLC first = PlcManager.getPlc(config, false);
			 final PLC second = PlcManager.getPlc(config, false);
			 final PLC direct = new PLC(config, false)) {
			assertNotSame(first, second);
			assertSame(first.getDriverManager(), second.getDriverManager());
			assertSame(first.getDriverManager(), direct.getDriverManager());
			assertTrue(first.getDriverManager().getProtocolCodes().contains("s7"));
			assertTrue(first.getDriverManager().getProtocolCodes().contains("opcua"));
			assertNull(first.getPlcConnection());
			first.close();
			assertTrue(first.isClosed());
			assertFalse(second.isClosed());
			assertFalse(direct.isClosed());
			try (final PLC later = PlcManager.getPlc(config, false)) {
				assertSame(first.getDriverManager(), later.getDriverManager());
				assertFalse(later.isClosed());
			}
		}
	}

	@Test
	void concurrentClientsShareOneDriverManager() throws Exception {
		final ConnectionConfig config = connectionConfig();
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS,
			new ArrayBlockingQueue<>(32));
		final CountDownLatch start = new CountDownLatch(1);
		final List<Future<DefaultPlcDriverManager>> tasks = new ArrayList<>(32);
		try {
			for (int index = 0; index < 32; index++) {
				tasks.add(executor.submit(() -> {
					start.await();
					try (final PLC plc = PlcManager.getPlc(config, false)) {
						return plc.getDriverManager();
					}
				}));
			}
			start.countDown();
			final DefaultPlcDriverManager expected = tasks.get(0).get(10, TimeUnit.SECONDS);
			for (final Future<DefaultPlcDriverManager> task : tasks) {
				assertSame(expected, task.get(10, TimeUnit.SECONDS));
			}
		} finally {
			start.countDown();
			executor.shutdownNow();
		}
	}

	/**
	 * 延迟连接测试仅初始化驱动管理器，不访问现场设备。
	 */
	private ConnectionConfig connectionConfig() {
		final ConnectionConfig config = mock(ConnectionConfig.class);
		when(config.protocol()).thenReturn(PlcProtocol.values()[0]);
		when(config.reconnectIntervalMillis()).thenReturn(5000L);
		return config;
	}
}
