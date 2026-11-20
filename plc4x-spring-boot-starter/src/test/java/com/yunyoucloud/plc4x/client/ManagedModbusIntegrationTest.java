package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import com.yunyoucloud.plc4x.exception.PlcReadExpection;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.spi.connection.AbstractPlcConnection;
import org.apache.plc4x.java.spi.connection.ChannelExposingConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 使用真实 Modbus TCP 帧覆盖设备无响应、返回异常、断线及晚到响应。
 */
@Timeout(20)
class ManagedModbusIntegrationTest {

	@Test
	void timeoutDoesNotBlockNextReadOrWriteOnSameSocket() throws Exception {
		try (final Peer peer = new Peer(Mode.DROP_FIRST);
			 final PLC plc = new PLC(config(peer))) {
			final PlcConnection original = plc.getPlcConnection();
			assertThrows(Exception.class, () -> plc.readShort("holding-register:1:INT"));
			assertEquals(1234, plc.readShort("holding-register:1:INT"));
			assertTrue(plc.writeShort("holding-register:1:INT", (short) -321));
			assertEquals(-321, plc.readShort("holding-register:1:INT"));
			assertSame(original, plc.getPlcConnection());
			assertEquals(1, peer.connections.get());
			assertInstanceOf(ManagedModbusProtocolLogic.class, ((AbstractPlcConnection) original).getProtocol());
		}
	}

	@Test
	void serverExceptionKeepsConnectionAndReportsReadFailureClearly() throws Exception {
		try (final Peer peer = new Peer(Mode.ERROR_FIRST);
			 final PLC plc = new PLC(config(peer))) {
			final PlcConnection original = plc.getPlcConnection();
			final PlcReadExpection error = assertThrows(PlcReadExpection.class,
					() -> plc.readShort("holding-register:1:INT"));
			assertTrue(error.getMessage().contains("status="));
			assertEquals(1234, plc.readShort("holding-register:1:INT"));
			assertSame(original, plc.getPlcConnection());
			assertEquals(1, peer.connections.get());
		}
	}

	@Test
	void physicalDisconnectReleasesOldConnectionAndReconnects() throws Exception {
		try (final Peer peer = new Peer(Mode.DISCONNECT_FIRST);
			 final PLC plc = new PLC(config(peer))) {
			final PlcConnection original = plc.getPlcConnection();
			assertThrows(Exception.class, () -> plc.readShort("holding-register:1:INT"));
			assertFalse(((ChannelExposingConnection) original).getChannel() != null
					&& ((ChannelExposingConnection) original).getChannel().isActive());
			assertEquals(1234, plc.readShort("holding-register:1:INT"));
			assertNotSame(original, plc.getPlcConnection());
			assertEquals(2, peer.connections.get());
		}
	}

	@Test
	void lateReplyCannotBeMistakenForNextTransaction() throws Exception {
		try (final Peer peer = new Peer(Mode.LATE_FIRST);
			 final PLC plc = new PLC(config(peer))) {
			assertThrows(Exception.class, () -> plc.readShort("holding-register:1:INT"));
			assertEquals(1234, plc.readShort("holding-register:1:INT"));
			assertEquals(1, peer.connections.get());
		}
	}

	private ConnectionConfig config(final Peer peer) {
		return new ConnectionConfig() {
			@Override
			public boolean enable() {
				return true;
			}

			@Override
			public String address() {
				return "modbus-tcp://127.0.0.1:" + peer.listener.getLocalPort()
						+ "?request-timeout=300&default-unit-identifier=1";
			}

			@Override
			public PlcProtocol protocol() {
				return PlcProtocol.MODBUS_TCP;
			}

			@Override
			public long requestTimeoutMillis() {
				return 1000;
			}

			@Override
			public long reconnectIntervalMillis() {
				return 0;
			}
		};
	}

	private enum Mode {
		DROP_FIRST, ERROR_FIRST, DISCONNECT_FIRST, LATE_FIRST
	}

	/**
	 * 仅实现本测试需要的 FC03、FC06、FC16，所有监听均限制在本机。
	 */
	private static final class Peer implements AutoCloseable {

		private final ServerSocket listener = new ServerSocket();
		private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(1), runnable -> {
			final Thread thread = new Thread(runnable, "modbus-test-peer");
			thread.setDaemon(true);
			return thread;
		});
		private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
		private final AtomicInteger connections = new AtomicInteger();
		private final AtomicInteger requests = new AtomicInteger();
		private final Mode mode;
		private volatile boolean closed;
		private volatile Throwable failure;
		private short value = 1234;

		private Peer(final Mode mode) throws IOException {
			this.mode = mode;
			listener.bind(new InetSocketAddress("127.0.0.1", 0));
			worker.execute(this::serve);
		}

		private void serve() {
			try {
				while (!closed) {
					try (final Socket socket = listener.accept()) {
						sockets.add(socket);
						connections.incrementAndGet();
						communicate(socket);
						sockets.remove(socket);
					}
				}
			} catch (SocketException failure) {
				if (!closed) {
					this.failure = failure;
				}
			} catch (IOException | InterruptedException failure) {
				if (!closed) {
					this.failure = failure;
				}
				Thread.currentThread().interrupt();
			}
		}

		private void communicate(final Socket socket) throws IOException, InterruptedException {
			final DataInputStream input = new DataInputStream(socket.getInputStream());
			final DataOutputStream output = new DataOutputStream(socket.getOutputStream());
			while (!closed) {
				final byte[] header = input.readNBytes(7);
				if (header.length == 0) {
					return;
				}
				if (header.length != 7) {
					throw new IOException("Short Modbus header");
				}
				final int length = (Byte.toUnsignedInt(header[4]) << 8) | Byte.toUnsignedInt(header[5]);
				final byte[] pdu = input.readNBytes(length - 1);
				final int index = requests.incrementAndGet();
				if (index == 1 && mode == Mode.DISCONNECT_FIRST) {
					return;
				}
				if (index == 1 && mode == Mode.DROP_FIRST) {
					continue;
				}
				final byte[] reply;
				if (index == 1 && mode == Mode.ERROR_FIRST) {
					reply = new byte[]{(byte) 0x83, 2};
				} else if (index == 1 && mode == Mode.LATE_FIRST) {
					TimeUnit.MILLISECONDS.sleep(450);
					reply = new byte[]{3, 2, 0, 111};
				} else if (pdu[0] == 3) {
					reply = new byte[]{3, 2, (byte) (value >> 8), (byte) value};
				} else if (pdu[0] == 6) {
					value = (short) ((Byte.toUnsignedInt(pdu[3]) << 8) | Byte.toUnsignedInt(pdu[4]));
					reply = pdu;
				} else if (pdu[0] == 16) {
					value = (short) ((Byte.toUnsignedInt(pdu[6]) << 8) | Byte.toUnsignedInt(pdu[7]));
					reply = java.util.Arrays.copyOf(pdu, 5);
				} else {
					throw new IOException("Unexpected Modbus function " + pdu[0]);
				}
				output.write(header, 0, 4);
				output.writeShort(reply.length + 1);
				output.writeByte(header[6]);
				output.write(reply);
				output.flush();
			}
		}

		@Override
		public void close() throws Exception {
			closed = true;
			listener.close();
			for (final Socket socket : sockets) {
				socket.close();
			}
			worker.shutdownNow();
			assertTrue(worker.awaitTermination(3, TimeUnit.SECONDS));
			assertNull(failure);
		}
	}
}
