package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.opcua.context.Conversation;
import org.apache.plc4x.java.opcua.context.SecureChannel;
import org.apache.plc4x.java.opcua.protocol.OpcuaProtocolLogic;
import org.apache.plc4x.java.opcua.protocol.OpcuaSubscriptionHandle;
import org.apache.plc4x.java.opcua.readwrite.NodeId;
import org.apache.plc4x.java.opcua.tag.OpcuaTag;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * PLC4X 0.13.1 没有公开 OPC UA 事务和保活清理接口，将版本适配集中在此处。
 */
final class Plc4xOpcuaAccess {

	private static final Field TRANSACTIONS = field(OpcuaProtocolLogic.class, "tm");
	private static final Field CONVERSATION = field(OpcuaProtocolLogic.class, "conversation");
	private static final Field CHANNEL = field(OpcuaProtocolLogic.class, "channel");
	private static final Field KEEP_ALIVE = field(SecureChannel.class, "keepAlive");
	private static final Field CONSUMERS = field(OpcuaSubscriptionHandle.class, "consumers");
	private static final Method PUBLISH = ReflectionUtils.findMethod(OpcuaSubscriptionHandle.class, "sendPublishRequest");
	private static final Method NODE_ID = ReflectionUtils.findMethod(OpcuaProtocolLogic.class, "generateNodeId", OpcuaTag.class);

	static {
		if (Objects.isNull(PUBLISH) || Objects.isNull(NODE_ID)) {
			throw new IllegalStateException("Unsupported PLC4X OPC UA subscription implementation");
		}
		ReflectionUtils.makeAccessible(PUBLISH);
		ReflectionUtils.makeAccessible(NODE_ID);
	}

	private Plc4xOpcuaAccess() {
	}

	/**
	 * 在协议实例交付和建连前替换尚未启动的事务管理器，不修改静态共享执行器。
	 */
	static void installTransactions(final OpcuaProtocolLogic protocol, final ManagedRequestTransactionManager manager) {
		((RequestTransactionManager) ReflectionUtils.getField(TRANSACTIONS, protocol)).shutdown();
		ReflectionUtils.setField(TRANSACTIONS, protocol, manager);
	}

	static Conversation conversation(final OpcuaProtocolLogic protocol) {
		return (Conversation) ReflectionUtils.getField(CONVERSATION, protocol);
	}

	static void installChannel(final OpcuaProtocolLogic protocol, final Conversation conversation,
							   final ManagedOpcuaSecureChannel channel) {
		ReflectionUtils.setField(CONVERSATION, protocol, conversation);
		ReflectionUtils.setField(CHANNEL, protocol, channel);
	}

	/**
	 * 连接关闭时立即取消本连接的保活任务，不能等待远端回应或关闭事务排到队首。
	 */
	static void cancelKeepAlive(final OpcuaProtocolLogic protocol) {
		final Object channel = ReflectionUtils.getField(CHANNEL, protocol);
		if (channel instanceof ManagedOpcuaSecureChannel managed) {
			managed.stopLocal();
		}
		if (Objects.nonNull(channel)) {
			final ScheduledFuture<?> task = (ScheduledFuture<?>) ReflectionUtils.getField(KEEP_ALIVE, channel);
			if (Objects.nonNull(task)) {
				task.cancel(false);
			}
		}
	}

	/**
	 * 消费者注册和通知发生在不同线程，使用并发集合并在停止后释放回调引用。
	 */
	static void installConcurrentConsumers(final OpcuaSubscriptionHandle handle) {
		ReflectionUtils.setField(CONSUMERS, handle, ConcurrentHashMap.newKeySet());
	}

	static void clearConsumers(final OpcuaSubscriptionHandle handle) {
		((Set<?>) ReflectionUtils.getField(CONSUMERS, handle)).clear();
	}

	@SuppressWarnings("unchecked")
	static Set<Consumer<PlcSubscriptionEvent>> consumers(final OpcuaSubscriptionHandle handle) {
		return (Set<Consumer<PlcSubscriptionEvent>>) ReflectionUtils.getField(CONSUMERS, handle);
	}

	/**
	 * 沿用上游的 Publish 报文和通知解码，由受管守护定时器触发。
	 */
	static void publish(final OpcuaSubscriptionHandle handle) {
		ReflectionUtils.invokeMethod(PUBLISH, handle);
	}

	static NodeId nodeId(final OpcuaTag tag) {
		return (NodeId) ReflectionUtils.invokeMethod(NODE_ID, null, tag);
	}

	private static Field field(final Class<?> type, final String name) {
		final Field field = ReflectionUtils.findField(type, name);
		if (Objects.isNull(field)) {
			throw new IllegalStateException("Unsupported PLC4X 0.13.1 OPC UA field: " + name);
		}
		ReflectionUtils.makeAccessible(field);
		return field;
	}
}
