package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.PlcDriver;
import org.apache.plc4x.java.modbus.tcp.ModbusTcpDriver;
import org.apache.plc4x.java.opcua.OpcuaPlcDriver;
import org.apache.plc4x.java.s7.readwrite.S7Driver;

import java.util.Map;

/**
 * 为项目公开的三类标准驱动应用适配，自定义注入的驱动不被替换。
 */
final class ManagedDrivers {

	private static final Map<Class<?>, PlcDriver> DRIVERS = Map.of(
			S7Driver.class, new ManagedS7Driver(),
			ModbusTcpDriver.class, new ManagedModbusDriver(),
			OpcuaPlcDriver.class, new ManagedOpcuaDriver());

	private ManagedDrivers() {
	}

	static PlcDriver select(final PlcDriver driver) {
		return DRIVERS.getOrDefault(driver.getClass(), driver);
	}
}
