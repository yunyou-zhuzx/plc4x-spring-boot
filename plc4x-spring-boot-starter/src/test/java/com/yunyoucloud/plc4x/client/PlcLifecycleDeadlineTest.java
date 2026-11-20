package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 验证超时后仍在执行的驱动不会被误判为已退出，也不会失去清理入口。
 */
@Timeout(10)
class PlcLifecycleDeadlineTest {

	private ConnectionConfig config;
	private DefaultPlcDriverManager manager;
	private PlcDriver driver;
	private PlcConnection connection;

	@BeforeEach
	void setUp() throws Exception {
		config = mock(ConnectionConfig.class);
		manager = mock(DefaultPlcDriverManager.class);
		driver = mock(PlcDriver.class);
		connection = mock(PlcConnection.class);
		when(config.protocol()).thenReturn(PlcProtocol.S7);
		when(config.address()).thenReturn("s7://localhost");
		when(config.connectTimeoutMillis()).thenReturn(1000L);
		when(config.closeTimeoutMillis()).thenReturn(1000L);
		when(config.reconnectIntervalMillis()).thenReturn(0L);
		when(config.requestTimeoutMillis()).thenReturn(5000L);
		when(manager.getDriverForUrl(anyString())).thenReturn(driver);
		when(driver.getConnection(anyString())).thenReturn(connection);
	}

	@Test
	void initialFailureExposesClientWhenCleanupAlsoFails() throws Exception {
		doThrow(new PlcConnectionException("handshake failed")).when(connection).connect();
		doThrow(new PlcConnectionException("cleanup failed")).when(connection).close();
		final PlcInitializationException failure = assertThrows(PlcInitializationException.class, () -> new PLC(config, manager));
		assertInstanceOf(PlcConnectionException.class, failure.getCause());
		final PLC retained = failure.getClient();
		assertSame(connection, retained.getPlcConnection());
		doNothing().when(connection).close();
		retained.close();
		assertNull(retained.getPlcConnection());
	}

	@Test
	void handshakeTimeoutDoesNotHoldClientLockForever() throws Exception {
		when(config.connectTimeoutMillis()).thenReturn(50L);
		final CountDownLatch exited = new CountDownLatch(1);
		doAnswer(invocation -> {
			try {
				new CountDownLatch(1).await();
			} finally {
				exited.countDown();
			}
			return null;
		}).when(connection).connect();
		final PLC plc = new PLC(config, manager, false);
		assertTimeout(Duration.ofSeconds(2), () -> assertFalse(plc.checkAndReconnectConnection()));
		assertTrue(exited.await(2, TimeUnit.SECONDS));
		retryClose(plc);
		assertNull(plc.getPlcConnection());
	}

	@Test
	void interruptIgnoringHandshakeCannotBeOverlappedOrDiscarded() throws Exception {
		when(config.connectTimeoutMillis()).thenReturn(50L);
		final CountDownLatch release = new CountDownLatch(1);
		final CountDownLatch interrupted = new CountDownLatch(1);
		doAnswer(invocation -> {
			awaitIgnoringInterrupt(release, interrupted);
			return null;
		}).when(connection).connect();
		final PlcInitializationException failure = assertThrows(PlcInitializationException.class, () -> new PLC(config, manager));
		final PLC retained = failure.getClient();
		try {
			assertTrue(interrupted.await(2, TimeUnit.SECONDS));
			assertTimeout(Duration.ofSeconds(1), () -> assertThrows(IllegalStateException.class, retained::close));
			assertSame(connection, retained.getPlcConnection());
			verify(connection, never()).close();
			verify(driver).getConnection(anyString());
		} finally {
			release.countDown();
			retryClose(retained);
		}
	}

	@Test
	void connectionCreationTimeoutRetainsOwnerBeforeConnectionObjectExists() throws Exception {
		when(config.connectTimeoutMillis()).thenReturn(50L);
		final CountDownLatch release = new CountDownLatch(1);
		final CountDownLatch interrupted = new CountDownLatch(1);
		when(driver.getConnection(anyString())).thenAnswer(invocation -> {
			awaitIgnoringInterrupt(release, interrupted);
			return connection;
		});
		final PlcInitializationException failure = assertThrows(PlcInitializationException.class, () -> new PLC(config, manager));
		final PLC retained = failure.getClient();
		try {
			assertTrue(interrupted.await(2, TimeUnit.SECONDS));
			assertNull(retained.getPlcConnection());
			assertThrows(IllegalStateException.class, retained::close);
			verify(connection, never()).connect();
		} finally {
			release.countDown();
			retryClose(retained);
		}
		verify(connection, never()).connect();
		verify(connection).close();
		assertNull(retained.getPlcConnection());
	}

	@Test
	void closeTimeoutKeepsOwnershipAndDoesNotStartConcurrentCleanup() throws Exception {
		when(config.closeTimeoutMillis()).thenReturn(50L);
		final PLC plc = new PLC(config, manager);
		final CountDownLatch release = new CountDownLatch(1);
		final CountDownLatch interrupted = new CountDownLatch(1);
		doAnswer(invocation -> {
			awaitIgnoringInterrupt(release, interrupted);
			return null;
		}).when(connection).close();
		try {
			assertTimeout(Duration.ofSeconds(1), () -> assertThrows(IllegalStateException.class, plc::close));
			assertTrue(interrupted.await(2, TimeUnit.SECONDS));
			assertThrows(IllegalStateException.class, plc::close);
			assertSame(connection, plc.getPlcConnection());
			verify(connection).close();
		} finally {
			release.countDown();
			retryClose(plc);
		}
	}

	/**
	 * 模拟不响应中断的第三方驱动，测试结束必须显式放行，不能遗留工作线程。
	 */
	private void awaitIgnoringInterrupt(final CountDownLatch release, final CountDownLatch interrupted) {
		boolean done = false;
		while (!done) {
			try {
				release.await();
				done = true;
			} catch (InterruptedException failure) {
				interrupted.countDown();
			}
		}
	}

	/**
	 * 放行驱动后重试清理，等待实际执行结束而非 Future 的取消标志。
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
