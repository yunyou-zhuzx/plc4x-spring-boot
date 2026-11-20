package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 精确注销业务回调，避免上游注册对象和协议 unregister() 相互递归。
 */
final class ManagedOpcuaConsumerRegistration implements PlcConsumerRegistration {

	private static final AtomicInteger SEQUENCE = new AtomicInteger();
	private final int id = SEQUENCE.incrementAndGet();
	private final List<ManagedOpcuaSubscription> handles;
	private final AtomicReference<Consumer<PlcSubscriptionEvent>> consumer;

	ManagedOpcuaConsumerRegistration(final Consumer<PlcSubscriptionEvent> callback,
									 final List<ManagedOpcuaSubscription> handles) {
		this.handles = List.copyOf(handles);
		consumer = new AtomicReference<>(Objects.requireNonNull(callback));
		final List<ManagedOpcuaSubscription> registered = new ArrayList<>();
		try {
			for (final ManagedOpcuaSubscription handle : handles) {
				handle.addConsumer(callback);
				registered.add(handle);
			}
		} catch (RuntimeException | Error failure) {
			registered.forEach(handle -> handle.removeConsumer(callback));
			consumer.set(null);
			throw failure;
		}
	}

	@Override
	public Integer getConsumerId() {
		return id;
	}

	@Override
	public List<PlcSubscriptionHandle> getSubscriptionHandles() {
		return List.copyOf(handles);
	}

	@Override
	public void unregister() {
		final Consumer<PlcSubscriptionEvent> callback = consumer.getAndSet(null);
		if (Objects.nonNull(callback)) {
			handles.forEach(handle -> handle.removeConsumer(callback));
		}
	}
}
