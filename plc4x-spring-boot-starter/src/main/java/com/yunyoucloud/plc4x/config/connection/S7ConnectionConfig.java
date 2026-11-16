package com.yunyoucloud.plc4x.config.connection;

import cn.hutool.core.util.StrUtil;
import com.yunyoucloud.plc4x.core.enums.PlcProtocol;
import lombok.Data;

@Data
public class S7ConnectionConfig implements ConnectionConfig {
	
	private Boolean enable = false;
	
	private String ip = "192.168.1.10";
	
	private Integer port = 102;
	
	private Integer rack = 0;
	
	private Integer slot = 1;
	
	private Integer pduLength = 240;
	
	private String controllerType = "ANY";
	
	@Override
	public boolean enable() {
		return this.enable;
	}
	
	@Override
	public String address() {
		return StrUtil.format(
			"s7://{}?remote-rack={}&remote-slot={}&pdu-size={}&controller-type={}",
			ip,
			rack,
			slot,
			pduLength,
			controllerType
		);
	}
	
	@Override
	public PlcProtocol protocol() {
		return PlcProtocol.S7;
	}
}
