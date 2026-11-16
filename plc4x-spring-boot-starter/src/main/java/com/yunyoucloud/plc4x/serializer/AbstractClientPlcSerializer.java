package com.yunyoucloud.plc4x.serializer;

import com.yunyoucloud.plc4x.client.PLC;
import com.yunyoucloud.plc4x.core.PlcParseData;
import com.yunyoucloud.plc4x.core.enums.EDataType;
import com.yunyoucloud.plc4x.exception.PlcCommExpection;
import com.yunyoucloud.plc4x.resolve.PlcResolve;
import lombok.SneakyThrows;
import org.apache.plc4x.java.api.messages.PlcReadResponse;

import java.util.Objects;

public abstract class AbstractClientPlcSerializer extends AbstractPlcSerializer {
	
	public AbstractClientPlcSerializer(final PLC plc) {
		super(plc);
	}
	
	@Override
	public PlcResolve getPlcResolve() {
		return new PlcResolve() {
			@Override
			public Object resolve(final PlcParseData plcParseData, final String tagName, final PlcReadResponse plcReadResponse) {
				return plc.resolvePlcValue(tagName, plcReadResponse, plcParseData.getDataType().getClazz());
			}
			
			@Override
			@SneakyThrows
			public Object extract(final Object targetFiledValueDb, final PlcParseData plcParseData) {
				return targetFiledValueDb;
			}
		};
	}
	
	@Override
	public boolean readBoolean(String address) {
		return singleRead(address, Boolean.class, EDataType.BOOL);
	}
	
	@Override
	public byte readByte(String address) {
		return singleRead(address, Byte.class, EDataType.BYTE);
	}
	
	@Override
	public short readShort(String address) {
		return singleRead(address, Short.class, EDataType.INT16);
	}
	
	@Override
	public int readInteger(String address) {
		return singleRead(address, Integer.class, EDataType.INT32);
	}
	
	@Override
	public long readLong(String address) {
		return singleRead(address, Long.class, EDataType.INT64);
	}
	
	@Override
	public float readFloat(String address) {
		return singleRead(address, Float.class, EDataType.FLOAT32);
	}
	
	@Override
	public double readDouble(String address) {
		return singleRead(address, Double.class, EDataType.FLOAT64);
	}
	
	@Override
	public String readString(String address) {
		return singleRead(address, String.class, EDataType.STRING);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeBoolean(final String address, final boolean value) {
		return singleWrite(address, value, EDataType.BOOL);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeByte(final String address, final byte value) {
		return singleWrite(address, value, EDataType.BYTE);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeShort(final String address, final short value) {
		return singleWrite(address, value, EDataType.INT16);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeInteger(final String address, final int value) {
		return singleWrite(address, value, EDataType.INT32);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeLong(final String address, final long value) {
		return singleWrite(address, value, EDataType.INT64);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeFloat(final String address, final float value) {
		return singleWrite(address, value, EDataType.FLOAT32);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeDouble(final String address, final double value) {
		return singleWrite(address, value, EDataType.FLOAT64);
	}
	
	/**
	 * 传递底层写入确认结果；不代表设备动作已经完成。
	 */
	@Override
	public boolean writeString(final String address, final String value) {
		return singleWrite(address, value, EDataType.STRING);
	}
	
	public <T> T singleRead(final String address, final Class<T> tClass, final EDataType eDataType) {
		if (eDataType == null) {
			throw new PlcCommExpection("类型不正确");
		}
		return plc.read(resolveAddress("", address, eDataType), tClass);
	}
	
	/**
	 * 转换地址和值后写入，并返回底层协议确认结果。
	 */
	public <T> boolean singleWrite(final String address, final T value, final EDataType eDataType) {
		if (Objects.isNull(eDataType)) {
			throw new PlcCommExpection("类型不正确");
		}
		final PlcParseData plcParseData = new PlcParseData();
		plcParseData.setDataType(eDataType);
		plcParseData.setBits(resolveBits(address));
		plcParseData.getRequestItem().setAddress(address);
		return plc.write(resolveAddress("", address, eDataType), getPlcResolve().extract(value, plcParseData));
	}
	
}
