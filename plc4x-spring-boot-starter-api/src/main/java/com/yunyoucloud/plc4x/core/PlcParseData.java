package com.yunyoucloud.plc4x.core;

import com.yunyoucloud.plc4x.core.enums.BitMode;
import com.yunyoucloud.plc4x.core.enums.EDataType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.lang.reflect.Field;

@Data
@EqualsAndHashCode
public class PlcParseData {
	
	private EDataType dataType;
	private int count;
	private Field field;
	private String bits;
	private BitMode bitMode;
	private RequestItem requestItem;
	private ResponseItem responseItem;
	
	// 后续扩展
	private int size = 0;
	private int countIndex = 0;
	private Class<?> targetClass;
	private EDataType targetType;
	
	public PlcParseData() {
		this.dataType = EDataType.AUTO;
		this.requestItem = new RequestItem();
		this.responseItem = new ResponseItem();
	}
	
}
