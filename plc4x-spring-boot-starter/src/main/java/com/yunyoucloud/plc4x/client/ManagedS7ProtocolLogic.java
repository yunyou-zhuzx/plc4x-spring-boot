/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.exceptions.PlcInvalidTagException;
import org.apache.plc4x.java.api.exceptions.PlcRuntimeException;
import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.s7.readwrite.*;
import org.apache.plc4x.java.s7.readwrite.tag.*;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.context.DriverContext;
import org.apache.plc4x.java.spi.generation.*;
import org.apache.plc4x.java.spi.messages.*;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager;
import org.apache.plc4x.java.spi.values.*;

import java.util.*;
import java.util.concurrent.*;

import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import org.apache.plc4x.java.s7.readwrite.protocol.S7ProtocolLogic;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * 限制 S7 事务和解码队列，解码异常被上游吞掉时也能结束外层响应等待。
 */
public class ManagedS7ProtocolLogic extends S7ProtocolLogic {

	private static final Field TRANSACTIONS = requiredField("tm");
	private static final Field DECODING = requiredField("clientExecutorService");
	private final ManagedRequestTransactionManager transactions = new ManagedRequestTransactionManager();
	private final ThreadPoolExecutor decoder;

	public ManagedS7ProtocolLogic() {
		final ExecutorService original = (ExecutorService) ReflectionUtils.getField(DECODING, this);
		original.shutdownNow();
		decoder = new ThreadPoolExecutor(4, 4, 1, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(64), runnable -> {
			final Thread thread = new Thread(runnable, "plc-s7-decode");
			thread.setDaemon(true);
			return thread;
		}, new ThreadPoolExecutor.AbortPolicy());
		decoder.allowCoreThreadTimeOut(true);
		ReflectionUtils.setField(DECODING, this, decoder);
	}

	@Override
	public void setDriverContext(final DriverContext context) {
		super.setDriverContext(context);
		((RequestTransactionManager) ReflectionUtils.getField(TRANSACTIONS, this)).shutdown();
		ReflectionUtils.setField(TRANSACTIONS, this, transactions);
	}

	@Override
	public CompletableFuture<PlcReadResponse> read(final PlcReadRequest request) {
		return transactions.track(() -> readNative(request), REQUEST_TIMEOUT.toMillis());
	}

	@Override
	public CompletableFuture<PlcWriteResponse> write(final PlcWriteRequest request) {
		return transactions.track(() -> writeNative(request), REQUEST_TIMEOUT.toMillis());
	}

	@Override
	public void close(final ConversationContext<TPKTPacket> context) {
		transactions.shutdown();
		decoder.shutdownNow();
		super.close(context);
	}

	@Override
	public void onDisconnect(final ConversationContext<TPKTPacket> context) {
		transactions.shutdown();
		super.onDisconnect(context);
	}

	/**
	 * 仅适配当前锁定的 PLC4X 实现，字段变化时明确失败，不能静默跳过资源管理。
	 */
	private static Field requiredField(final String name) {
		final Field field = ReflectionUtils.findField(S7ProtocolLogic.class, name);
		if (Objects.isNull(field)) {
			throw new IllegalStateException("Unsupported PLC4X S7 implementation: " + name);
		}
		ReflectionUtils.makeAccessible(field);
		return field;
	}

	private CompletableFuture<PlcReadResponse> readNative(final PlcReadRequest readRequest) {

		if (!(Boolean) access("isConnected")) {
			CompletableFuture<PlcReadResponse> future = new CompletableFuture<>();
			future.completeExceptionally(new PlcRuntimeException("Disconnected"));
			return future;
		}

		DefaultPlcReadRequest request = (DefaultPlcReadRequest) readRequest;
		CompletableFuture<S7Message> responseFuture;
		if (request.getTagNames().stream().anyMatch(t -> request.getTag(t) instanceof S7SzlTag)) {

			S7SzlTag szlTag = (S7SzlTag) request.getTags().get(0);
			S7Message s7Message = new S7MessageUserData((Integer) access("getTpduId"),
					new S7ParameterUserData(List.of(
							new S7ParameterUserDataItemCPUFunctions(
									(short) 0x11, (byte) 0x4, (byte) 0x4, (short) 0x01, (short) 0x00,
									null, null, null)
					)),
					new S7PayloadUserData(List.of(
							new S7PayloadUserDataItemCpuFunctionReadSzlRequest(DataTransportErrorCode.OK,
									DataTransportSize.OCTET_STRING,
									0x04,
									new SzlId(SzlModuleTypeClass.enumForValue((byte) ((szlTag.getSzlId() & 0xf000) >> 12)),
											(byte) ((szlTag.getSzlId() & 0x0f00) >> 8),
											SzlSublist.enumForValue((short) (szlTag.getSzlId() & 0x00ff))),
									szlTag.getIndex())
					)));
			responseFuture = access("sendInternal", s7Message);
		} else if (request.getTagNames().stream().anyMatch(t -> request.getTag(t) instanceof S7AckTag)) {
			responseFuture = access("performAlarmAckRequest", request);
		} else if (request.getTagNames().stream().anyMatch(t -> request.getTag(t) instanceof S7ClkTag)) {
			responseFuture = access("performClkRequest", request);
		} else if (request.getTagNames().stream().anyMatch(t -> request.getTag(t) instanceof S7StringVarLengthTag)) {
			responseFuture = access("performVarLengthStringReadRequest", request);
		} else {
			responseFuture = access("performOrdinaryReadRequest", request);
		}


		return responseFuture.thenApplyAsync(packet -> (PlcReadResponse) access("decodeReadResponse", packet, readRequest), decoder);
	}

	private CompletableFuture<PlcWriteResponse> writeNative(final PlcWriteRequest writeRequest) {

		if (!(Boolean) access("isConnected")) {
			CompletableFuture<PlcWriteResponse> future = new CompletableFuture<>();
			future.completeExceptionally(new PlcRuntimeException("Disconnected"));
			return future;
		}

		DefaultPlcWriteRequest request = (DefaultPlcWriteRequest) writeRequest;
		for (final String name : request.getTagNames()) {
			if (request.getTag(name) instanceof S7StringFixedLengthTag fixed) {
				validateStringCapacity(name, fixed, request.getPlcValue(name), fixed.getStringLength());
			}
		}
		CompletableFuture<S7Message> responseFuture = new CompletableFuture<>();

		if (request.getTagNames().stream().anyMatch(t -> request.getTag(t) instanceof S7ClkTag)) {
			responseFuture = access("performClkSetRequest", (DefaultPlcWriteRequest) writeRequest);
		} else if (request.getTagNames().stream().anyMatch(t -> request.getTag(t) instanceof S7StringVarLengthTag)) {
			responseFuture = writeVariableStrings((DefaultPlcWriteRequest) writeRequest);
		} else {
			responseFuture = access("performOrdinaryWriteRequest", request);
		}


		return responseFuture.thenApplyAsync(packet -> (PlcWriteResponse) access("decodeWriteResponse", packet, writeRequest), decoder);
	}

	/**
	 * 写入必须使用设备声明的最大容量，不能用旧值长度限制新值，否则会错误截断并修改容量头。
	 */
	private CompletableFuture<S7Message> writeVariableStrings(final DefaultPlcWriteRequest request) {
		final CompletableFuture<Map<S7StringVarLengthTag, StringSizes>> sizes = access("getStringSizes", request);
		return sizes.thenCompose(lengths -> {
			final LinkedHashMap<String, org.apache.plc4x.java.spi.messages.utils.PlcTagValueItem<org.apache.plc4x.java.api.model.PlcTag>> items = new LinkedHashMap<>();
			for (final String name : request.getTagNames()) {
				final org.apache.plc4x.java.api.model.PlcTag original = request.getTag(name);
				final org.apache.plc4x.java.api.value.PlcValue value = request.getPlcValue(name);
				org.apache.plc4x.java.api.model.PlcTag target = original;
				if (original instanceof S7StringVarLengthTag variable) {
					final StringSizes length = Objects.requireNonNull(lengths.get(variable), "Missing S7 string capacity");
					final int capacity = length.getMaxLength();
					validateStringCapacity(name, variable, value, capacity);
					target = new S7StringFixedLengthTag(variable.getDataType(), variable.getMemoryArea(), variable.getBlockNumber(),
							variable.getByteOffset(), variable.getBitOffset(), variable.getNumberOfElements(), capacity);
				}
				items.put(name, new org.apache.plc4x.java.spi.messages.utils.DefaultPlcTagValueItem<>(target, value));
			}
			return access("performOrdinaryWriteRequest", new DefaultPlcWriteRequest(request.getWriter(), items));
		});
	}

	/**
	 * STRING 按 UTF-8 字节数、WSTRING 按 UTF-16 代码单元检查，数组逐个检查，禁止静默截断。
	 */
	private void validateStringCapacity(final String name, final S7Tag tag,
										final org.apache.plc4x.java.api.value.PlcValue value, final int capacity) {
		for (int index = 0; index < tag.getNumberOfElements(); index++) {
			final String text = value.getIndex(index).getString();
			final int length = tag.getDataType() == TransportSize.STRING
					? text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : text.length();
			if (length > capacity) {
				throw new PlcInvalidTagException("String exceeds PLC capacity: " + name + ", capacity=" + capacity);
			}
		}
	}

	/**
	 * 所有订阅以异步响应衔接，不再占用解码线程执行无期限 get()。
	 */
	@Override
	public CompletableFuture<PlcSubscriptionResponse> subscribe(final PlcSubscriptionRequest request) {
		return transactions.track(() -> {
			if (!(Boolean) access("isConnected") || !(Boolean) access("isFeatureSupported")) {
				return CompletableFuture.failedFuture(new PlcRuntimeException("Disconnected or subscription unsupported"));
			}
			final DefaultPlcSubscriptionRequest subscription = (DefaultPlcSubscriptionRequest) request;
			final S7SubscriptionTag tag = (S7SubscriptionTag) subscription.getTags().get(0).getTag();
			final String encoder = switch (tag.getTagType()) {
				case EVENT_SUBSCRIPTION -> "encodeEventSubscriptionRequest";
				case ALARM_QUERY -> "encodeAlarmQueryRequest";
				case CYCLIC_SUBSCRIPTION -> "encodeCycledS7ANYSubscriptionRequest";
				case CYCLIC_DB_SUBSCRIPTION -> "encodeCycledDBREADSubscriptionRequest";
				default -> throw new PlcInvalidTagException("Unsupported subscription type: " + tag.getTagType());
			};
			final int id = access("getTpduId");
			final S7Message message = access(encoder, subscription, id);
			final TPKTPacket packet = new TPKTPacket(new COTPPacketData(null, message, true, (byte) id));
			final CompletableFuture<S7Message> wire = new CompletableFuture<>();
			final var transaction = transactions.startRequest();
			transaction.submit(() -> conversationContext.sendRequest(packet)
					.onTimeout(wire::completeExceptionally)
					.onError((sent, failure) -> wire.completeExceptionally(failure))
					.expectResponse(TPKTPacket.class, REQUEST_TIMEOUT)
					.unwrap(TPKTPacket::getPayload).only(COTPPacketData.class).unwrap(COTPPacket::getPayload)
					.check(response -> response.getTpduReference() == id)
					.handle(response -> {
						transaction.endRequest();
						wire.complete(response);
					}));
			return wire.thenApplyAsync(response -> {
				final S7ParameterUserData parameters = (S7ParameterUserData) response.getParameter();
				final S7ParameterUserDataItemCPUFunctions parameter = (S7ParameterUserDataItemCPUFunctions) parameters.getItems().get(0);
				return (PlcSubscriptionResponse) access("decodeEventSubscriptionResponse",
						Integer.toString(parameter.getSequenceNumber()), request, response);
			}, decoder);
		}, REQUEST_TIMEOUT.toMillis());
	}

	@Override
	public CompletableFuture<PlcUnsubscriptionResponse> unsubscribe(final PlcUnsubscriptionRequest request) {
		return transactions.track(() -> super.unsubscribe(request), REQUEST_TIMEOUT.toMillis());
	}

	/**
	 * 复用锁定版本的私有编码入口，反射边界保留原始异常原因。
	 */
	@SuppressWarnings("unchecked")
	protected <T> T access(final String name, final Object... arguments) {
		final Method[] found = new Method[1];
		ReflectionUtils.doWithMethods(S7ProtocolLogic.class, method -> {
			if (Objects.equals(method.getName(), name) && method.getParameterCount() == arguments.length) {
				final Class<?>[] types = method.getParameterTypes();
				for (int index = 0; index < types.length; index++) {
					if (!ClassUtils.isAssignableValue(types[index], arguments[index])) {
						return;
					}
				}
				found[0] = method;
			}
		});
		if (Objects.isNull(found[0])) {
			throw new IllegalStateException("Unsupported PLC4X S7 method: " + name);
		}
		ReflectionUtils.makeAccessible(found[0]);
		try {
			return (T) found[0].invoke(this, arguments);
		} catch (InvocationTargetException failure) {
			throw new CompletionException(failure.getCause());
		} catch (IllegalAccessException failure) {
			throw new IllegalStateException("Cannot access PLC4X S7 method: " + name, failure);
		}
	}

}
