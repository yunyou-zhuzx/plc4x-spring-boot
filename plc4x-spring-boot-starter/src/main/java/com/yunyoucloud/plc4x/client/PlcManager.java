package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;

public class PlcManager {

	public static PLC getPlc(final ConnectionConfig connectionConfig) {
		return getPlc(connectionConfig, true);
	}

	public static PLC getPlc(final ConnectionConfig connectionConfig, final boolean connectImmediately) {
		return new PLC(connectionConfig, connectImmediately);
	}

	public static boolean reconnect(final PLC plc) {
		return plc.checkAndReconnectConnection();
	}

	public static void close(final PLC plc) {
		plc.close();
	}
}
