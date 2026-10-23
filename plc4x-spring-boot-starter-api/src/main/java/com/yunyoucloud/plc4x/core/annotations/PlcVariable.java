package com.yunyoucloud.plc4x.core.annotations;

import com.yunyoucloud.plc4x.core.enums.BitMode;
import com.yunyoucloud.plc4x.core.enums.EDataType;
import com.yunyoucloud.plc4x.core.enums.PlcDisplayType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author yykj
 */
@Documented
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PlcVariable {
	
	/**
	 * 地址
	 *
	 * @return 地址
	 */
	String address();
	
	/**
	 * 数据类型
	 *
	 * @return 数据类型
	 */
	EDataType type() default EDataType.BYTE;
	
	/**
	 * 数量
	 *
	 * @return 数量
	 */
	int count() default 1;
	
	/**
	 * 尺寸
	 *
	 * @return 尺寸
	 */
	int size() default 0;
	
	/**
	 * bit 位模式
	 *
	 * @return bit 位模式
	 */
	BitMode bitMode() default BitMode.LOW;
	
	/**
	 * 是否需要显示
	 *
	 * @return 是否需要显示
	 */
	boolean needDisplay() default true;
	
	/**
	 * 显示
	 *
	 * @return 显示
	 */
	String display() default "";
	
	/**
	 * 显示类型
	 *
	 * @return 显示类型
	 */
	PlcDisplayType displayType() default PlcDisplayType.STR;
	
	/**
	 * 显示配置
	 *
	 * @return 显示配置
	 */
	String displayConfig() default "";
	
	/**
	 * 目标数据类型
	 *
	 * @return 目标数据类型
	 */
	EDataType targetType() default EDataType.AUTO;
	
	/**
	 * 目标数据类型
	 *
	 * @return 目标数据类型
	 */
	Class<?> targetClass() default EDataType.class;
	
	/**
	 * 字段排序
	 *
	 * @return 字段排序
	 */
	int sort() default 0;
	
	/**
	 * 是否只读
	 *
	 * @return 是否只读
	 */
	boolean onlyRead() default false;
}
