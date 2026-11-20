/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yunyoucloud.plc4x.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.types.PlcSubscriptionType;
import org.apache.plc4x.java.opcua.context.Conversation;
import org.apache.plc4x.java.opcua.protocol.OpcuaSubscriptionHandle;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.opcua.tag.OpcuaTag;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 监控项编码沿用 Apache PLC4X，修正本地发布任务取消依赖删除订阅成功的问题。
 */
@Slf4j
final class ManagedOpcuaSubscription extends OpcuaSubscriptionHandle {

	/**
	 * 只调度受管订阅；取消后立即移除任务，守护线程不阻止应用退出。
	 */
	private static final ScheduledThreadPoolExecutor PUBLISHER = new ScheduledThreadPoolExecutor(1, runnable -> {
		final Thread thread = new Thread(runnable, "plc-opcua-publish");
		thread.setDaemon(true);
		return thread;
	});

	static {
		PUBLISHER.setRemoveOnCancelPolicy(true);
		PUBLISHER.setKeepAliveTime(1, TimeUnit.SECONDS);
		PUBLISHER.allowCoreThreadTimeOut(true);
	}

	private final ManagedOpcuaProtocolLogic owner;
	private final ManagedRequestTransactionManager transactions;
	private final Conversation conversation;
	private final Spec spec;
	private final Object lock = new Object();
	private ScheduledFuture<?> task;
	private boolean stopped;
	private CompletableFuture<Void> stopping;
	private CompletableFuture<Void> publishing;
	private final Map<Consumer<PlcSubscriptionEvent>, Integer> consumers = new HashMap<>();

	ManagedOpcuaSubscription(final ManagedOpcuaProtocolLogic owner, final ManagedRequestTransactionManager transactions,
							 final Conversation conversation, final Spec spec) {
		super(owner, transactions, conversation, spec.request(), spec.id(), spec.cycleMillis());
		this.owner = owner;
		this.transactions = transactions;
		this.conversation = conversation;
		this.spec = spec;
		Plc4xOpcuaAccess.installConcurrentConsumers(this);
	}

	@Override
	public CompletableFuture<OpcuaSubscriptionHandle> onSubscribeCreateMonitoredItemsRequest() {
		final List<MonitoredItemCreateRequest> items = new ArrayList<>();
		long clientHandle = 1;
		for (final String name : spec.request().getTagNames()) {
			final DefaultPlcSubscriptionTag subscription = (DefaultPlcSubscriptionTag) spec.request().getTag(name);
			final OpcuaTag tag = (OpcuaTag) subscription.getTag();
			final MonitoringParameters parameters = new MonitoringParameters(clientHandle++, (double) spec.samplingMillis(),
					filter(subscription, tag), 1L, true);
			items.add(new MonitoredItemCreateRequest(readValue(subscription, tag),
					MonitoringMode.monitoringModeReporting, parameters));
		}
		final CreateMonitoredItemsRequest request = new CreateMonitoredItemsRequest(conversation.createRequestHeader(),
				spec.id(), TimestampsToReturn.timestampsToReturnBoth, items);
		return conversation.submit(request, CreateMonitoredItemsResponse.class).thenApply(response -> {
			if (response.getResults().size() != items.size()
					|| response.getResults().stream().anyMatch(item -> (item.getStatusCode().getStatusCode() & 0xc0000000L) != 0)) {
				throw new IllegalStateException("OPC UA server rejected one or more monitored items");
			}
			synchronized (lock) {
				if (stopped) {
					throw new CancellationException("OPC UA subscription stopped during creation");
				}
				task = PUBLISHER.scheduleAtFixedRate(this::publish, spec.cycleMillis() / 2,
						spec.cycleMillis(), TimeUnit.MILLISECONDS);
			}
			return this;
		});
	}

	@Override
	public void stopSubscriber() {
		stop().exceptionally(failure -> {
			log.debug("OPC UA 远端订阅删除失败，本地发布任务已停止", failure);
			return null;
		});
	}

	/**
	 * 先停止本地任务，再删除服务器订阅；执行中及成功结果复用，失败后允许重试。
	 */
	CompletableFuture<Void> stop() {
		synchronized (lock) {
			stopLocal();
			if (Objects.isNull(stopping) || stopping.isCompletedExceptionally()) {
				stopping = owner.deleteSubscription(this);
			}
			return stopping;
		}
	}

	/**
	 * 断链和关闭路径无需等待网络，即可解除调度器对订阅和业务回调的持有。
	 */
	void stopLocal() {
		synchronized (lock) {
			stopped = true;
			if (Objects.nonNull(publishing)) {
				publishing.cancel(false);
				publishing = null;
			}
			if (Objects.nonNull(task)) {
				task.cancel(false);
				task = null;
			}
			Plc4xOpcuaAccess.clearConsumers(this);
			consumers.clear();
		}
	}

	boolean isPublishing() {
		synchronized (lock) {
			return Objects.nonNull(task) && !task.isCancelled();
		}
	}

	boolean belongsTo(final ManagedOpcuaProtocolLogic protocol) {
		return Objects.equals(owner, protocol);
	}

	@Override
	public PlcConsumerRegistration register(final Consumer<PlcSubscriptionEvent> consumer) {
		return new ManagedOpcuaConsumerRegistration(consumer, List.of(this));
	}

	void addConsumer(final Consumer<PlcSubscriptionEvent> consumer) {
		synchronized (lock) {
			if (stopped) {
				throw new IllegalStateException("OPC UA subscription has stopped");
			}
			consumers.merge(consumer, 1, Integer::sum);
			Plc4xOpcuaAccess.consumers(this).add(consumer);
		}
	}

	void removeConsumer(final Consumer<PlcSubscriptionEvent> consumer) {
		synchronized (lock) {
			final Integer count = consumers.get(consumer);
			if (Objects.isNull(count) || count <= 1) {
				consumers.remove(consumer);
				Plc4xOpcuaAccess.consumers(this).remove(consumer);
			} else {
				consumers.put(consumer, count - 1);
			}
		}
	}

	private void publish() {
		synchronized (lock) {
			if (stopped || Objects.nonNull(publishing) && !publishing.isDone()) {
				return;
			}
			// 同一订阅只保留一个在途 Publish，慢响应不能挤满整个连接的事务队列。
			publishing = transactions.trackOperation(() -> Plc4xOpcuaAccess.publish(this), owner.requestTimeoutMillis());
			publishing.whenComplete((ignored, failure) -> {
				if (Objects.nonNull(failure) && !(failure instanceof CancellationException)) {
					log.debug("OPC UA 本次发布失败，后续周期继续使用原连接", failure);
				}
			});
		}
	}

	/**
	 * 变量和事件订阅保留原有地址及事件过滤语义。
	 */
	private ReadValueId readValue(final DefaultPlcSubscriptionTag subscription, final OpcuaTag tag) {
		if (subscription.getPlcSubscriptionType() == PlcSubscriptionType.EVENT) {
			final NodeId base = new NodeId(new NodeIdFourByte((short) 0, OpcuaNodeIdServicesObjectType.BaseEventType.getValue()));
			return new ReadValueId(base, AttributeId.EventNotifier.getValue(), new PascalString(null),
					new QualifiedName(0, new PascalString(null)));
		}
		return new ReadValueId(Plc4xOpcuaAccess.nodeId(tag), tag.getAttributeId().getValue(),
				new PascalString(null), new QualifiedName(0, new PascalString(null)));
	}

	private ExtensionObject filter(final DefaultPlcSubscriptionTag subscription, final OpcuaTag tag) {
		final ExpandedNodeId empty = new ExpandedNodeId(false, false, new NodeIdTwoByte((short) 0), null, null);
		if (subscription.getPlcSubscriptionType() != PlcSubscriptionType.EVENT) {
			return new NullExtensionObjectWithMask(empty, new ExtensionObjectEncodingMask(false, false, false));
		}
		final NodeId base = new NodeId(new NodeIdFourByte((short) 0, OpcuaNodeIdServicesObjectType.BaseEventType.getValue()));
		final List<SimpleAttributeOperand> operands = new ArrayList<>();
		for (final Map.Entry<String, String> entry : tag.getConfig().entrySet()) {
			operands.add(new SimpleAttributeOperand(base, List.of(new QualifiedName(0, new PascalString(entry.getKey()))),
					AttributeId.Value.getValue(), new PascalString(null)));
		}
		final EventFilter filter = new EventFilter(operands, new ContentFilter(null));
		final ExpandedNodeId type = new ExpandedNodeId(false, false,
				new NodeIdFourByte((short) 0, filter.getExtensionId()), null, null);
		return new BinaryExtensionObjectWithMask(type, new ExtensionObjectEncodingMask(false, false, true), filter);
	}

	/**
	 * 一个服务器订阅对应的一组监控项和周期。
	 */
	record Spec(PlcSubscriptionRequest request, long id, long cycleMillis, long samplingMillis) {
		Spec(final PlcSubscriptionRequest request, final long id, final long cycleMillis) {
			this(request, id, cycleMillis, cycleMillis);
		}
	}
}
