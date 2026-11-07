package com.yunyoucloud.plc4x.client;

import com.yunyoucloud.plc4x.config.connection.ConnectionConfig;

public class PlcManager {

	public static PLC getPlc(final ConnectionConfig connectionConfig) {
		return new PLC(connectionConfig);
	}

	public static boolean reconnect(final PLC plc) {
		return plc.checkAndReconnectConnection();
	}

	public static void close(final PLC plc) {
		plc.close();
	}
}
