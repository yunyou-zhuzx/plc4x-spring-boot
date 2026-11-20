package com.yunyoucloud.plc4x.client;

import cn.hutool.core.collection.CollUtil;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.opcua.config.OpcuaConfiguration;
import org.apache.plc4x.java.opcua.context.Conversation;
import org.apache.plc4x.java.opcua.protocol.OpcuaProtocolLogic;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.configuration.HasConfiguration;
import org.apache.plc4x.java.spi.messages.DefaultPlcSubscriptionResponse;
import org.apache.plc4x.java.spi.messages.DefaultPlcUnsubscriptionResponse;
import org.apache.plc4x.java.spi.messages.utils.DefaultPlcResponseItem;
import org.apache.plc4x.java.spi.messages.utils.PlcResponseItem;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 管理 OPC UA 的请求、原生订阅和保活资源，不以服务端请求错误关闭 TCP。
 */
public class ManagedOpcuaProtocolLogic extends OpcuaProtocolLogic implements HasConfiguration<OpcuaConfiguration> {

	private final ManagedRequestTransactionManager transactions = new ManagedRequestTransactionManager();
	private final Map<Long, ManagedOpcuaSubscription> subscriptions = new ConcurrentHashMap<>();
	private volatile boolean closing;
	private long requestTimeoutMillis = 30000;
	private OpcuaConfiguration configuration;
	private org.apache.plc4x.java.opcua.context.OpcuaDriverContext context;
	private boolean channelCreated;

	@Override
	public void setDriverContext(final org.apache.plc4x.java.spi.context.DriverContext context) {
		super.setDriverContext(context);
		this.context = (org.apache.plc4x.java.opcua.context.OpcuaDriverContext) context;
	}

	private void ensureChannel(final ConversationContext<OpcuaAPU> context) {
		if (!channelCreated) {
			final Conversation conversation = new ManagedOpcuaConversation(context, this.context, configuration);
			Plc4xOpcuaAccess.installChannel(this, conversation,
					new ManagedOpcuaSecureChannel(conversation, transactions, this.context, configuration, context.getAuthentication()));
			channelCreated = true;
		}
	}

	@Override
	public void onConnect(final ConversationContext<OpcuaAPU> context) {
		ensureChannel(context);
		super.onConnect(context);
	}

	@Override
	public void onDiscover(final ConversationContext<OpcuaAPU> context) {
		if (configuration.isDiscovery()) {
			ensureChannel(context);
		}
		super.onDiscover(context);
	}

	public ManagedOpcuaProtocolLogic() {
		Plc4xOpcuaAccess.installTransactions(this, transactions);
	}

	void receive(final OpcuaAPU packet, final Runnable dispatch) throws org.apache.plc4x.java.api.exceptions.PlcProtocolException {
		final Conversation conversation = Plc4xOpcuaAccess.conversation(this);
		if (conversation instanceof ManagedOpcuaConversation managed) {
			managed.receive(packet, dispatch);
		} else {
			dispatch.run();
		}
	}

	void rejectReceived(final Throwable failure) {
		transactions.failPendingResponses(failure);
	}

	long requestTimeoutMillis() {
		return requestTimeoutMillis;
	}

	@Override
	public void setConfiguration(final OpcuaConfiguration configuration) {
		super.setConfiguration(configuration);
		this.configuration = configuration;
		if (configuration.getRequestTimeout() <= 0) {
			throw new IllegalArgumentException("OPC UA request timeout must be positive");
		}
		requestTimeoutMillis = configuration.getRequestTimeout();
	}

	@Override
	public CompletableFuture<PlcReadResponse> read(final PlcReadRequest request) {
		if (closing) {
			return CompletableFuture.failedFuture(new PlcConnectionException("OPC UA connection is closing"));
		}
		return transactions.track(() -> super.read(request), requestTimeoutMillis);
	}

	@Override
	public CompletableFuture<PlcWriteResponse> write(final PlcWriteRequest request) {
		if (closing) {
			return CompletableFuture.failedFuture(new PlcConnectionException("OPC UA connection is closing"));
		}
		return transactions.track(() -> super.write(request), requestTimeoutMillis);
	}

	@Override
	public CompletableFuture<PlcSubscriptionResponse> subscribe(final PlcSubscriptionRequest request) {
		if (closing || CollUtil.isEmpty(request.getTagNames())) {
			return CompletableFuture.failedFuture(new IllegalStateException("OPC UA subscription is closed or empty"));
		}
		final long cycle = request.getTag(request.getTagNames().iterator().next())
				.getDuration().orElse(Duration.ofMillis(1000)).toMillis();
		if (cycle < 1 || cycle > Long.MAX_VALUE / 10) {
			return CompletableFuture.failedFuture(new IllegalArgumentException("OPC UA subscription cycle must be positive"));
		}
		final AtomicReference<ManagedOpcuaSubscription> created = new AtomicReference<>();
		final CompletableFuture<PlcSubscriptionResponse> result = transactions.track(() -> {
			final CompletableFuture<PlcSubscriptionResponse> future = new CompletableFuture<>();
			final var transaction = transactions.startRequest();
			transaction.submit(() -> {
				final Conversation conversation = Plc4xOpcuaAccess.conversation(this);
				final CreateSubscriptionRequest create = new CreateSubscriptionRequest(conversation.createRequestHeader(),
						cycle, 12000, 5, 65536, true, (short) 0);
				conversation.submit(create, CreateSubscriptionResponse.class).thenCompose(response -> {
					final double revised = response.getRevisedPublishingInterval();
					if (!Double.isFinite(revised) || revised < 1 || revised > Long.MAX_VALUE / 10) {
						return CompletableFuture.failedFuture(new IllegalStateException("Invalid OPC UA revised publishing interval"));
					}
					final ManagedOpcuaSubscription handle = new ManagedOpcuaSubscription(this, transactions, conversation,
							new ManagedOpcuaSubscription.Spec(request, response.getSubscriptionId(), (long) Math.ceil(revised), cycle));
					created.set(handle);
					subscriptions.put(handle.getSubscriptionId(), handle);
					if (closing || future.isDone()) {
						handle.stopSubscriber();
						return CompletableFuture.failedFuture(new IllegalStateException("OPC UA subscription cancelled during creation"));
					}
					return handle.onSubscribeCreateMonitoredItemsRequest();
				}).whenComplete((handle, failure) -> {
					if (Objects.nonNull(failure)) {
						future.completeExceptionally(failure);
					} else {
						final Map<String, PlcResponseItem<PlcSubscriptionHandle>> values = new HashMap<>();
						for (final String name : request.getTagNames()) {
							values.put(name, new DefaultPlcResponseItem<>(PlcResponseCode.OK, handle));
						}
						future.complete(new DefaultPlcSubscriptionResponse(request, values));
					}
				});
			});
			return future;
		}, requestTimeoutMillis);
		result.whenComplete((response, failure) -> {
			if (Objects.nonNull(failure) && Objects.nonNull(created.get())) {
				created.get().stopSubscriber();
			}
		});
		return result;
	}

	@Override
	public CompletableFuture<PlcUnsubscriptionResponse> unsubscribe(final PlcUnsubscriptionRequest request) {
		final List<CompletableFuture<Void>> results = new ArrayList<>();
		for (final PlcSubscriptionHandle handle : request.getSubscriptionHandles()) {
			if (!(handle instanceof ManagedOpcuaSubscription managed) || !managed.belongsTo(this)) {
				return CompletableFuture.failedFuture(new IllegalArgumentException("OPC UA subscription belongs to another connection"));
			}
			results.add(managed.stop());
		}
		return CompletableFuture.allOf(results.toArray(CompletableFuture[]::new))
				.thenApply(ignored -> new DefaultPlcUnsubscriptionResponse(request));
	}

	/**
	 * 本地停止已经完成，远端删除结果单独返回给调用方。
	 */
	CompletableFuture<Void> deleteSubscription(final ManagedOpcuaSubscription handle) {
		final CompletableFuture<Void> result = transactions.track(() -> {
			final CompletableFuture<Void> future = new CompletableFuture<>();
			final var transaction = transactions.startRequest();
			transaction.submit(() -> {
				final Conversation conversation = Plc4xOpcuaAccess.conversation(this);
				final DeleteSubscriptionsRequest delete = new DeleteSubscriptionsRequest(conversation.createRequestHeader(),
						List.of(handle.getSubscriptionId()));
				conversation.submit(delete, DeleteSubscriptionsResponse.class).whenComplete((response, failure) -> {
					if (Objects.nonNull(failure)) {
						future.completeExceptionally(failure);
					} else if (response.getResults().size() != 1
							|| ((response.getResults().get(0).getStatusCode() & 0xc0000000L) != 0
							&& response.getResults().get(0).getStatusCode() != OpcuaStatusCode.BadSubscriptionIdInvalid.getValue())) {
						future.completeExceptionally(new IllegalStateException("OPC UA server did not confirm subscription deletion"));
					} else {
						future.complete(null);
					}
				});
			});
			return future;
		}, requestTimeoutMillis);
		result.whenComplete((ignored, failure) -> subscriptions.remove(handle.getSubscriptionId(), handle));
		return result;
	}

	@Override
	public void removeSubscription(final Long id) {
		final ManagedOpcuaSubscription handle = subscriptions.remove(id);
		if (Objects.nonNull(handle)) {
			handle.stopLocal();
		}
	}

	@Override
	public PlcConsumerRegistration register(final Consumer<PlcSubscriptionEvent> consumer,
											final Collection<PlcSubscriptionHandle> handles) {
		final List<ManagedOpcuaSubscription> owned = new ArrayList<>();
		for (final PlcSubscriptionHandle handle : handles) {
			if (!(handle instanceof ManagedOpcuaSubscription managed) || !managed.belongsTo(this)) {
				throw new IllegalArgumentException("OPC UA subscription belongs to another connection");
			}
			owned.add(managed);
		}
		return new ManagedOpcuaConsumerRegistration(consumer, owned);
	}

	@Override
	public void unregister(final PlcConsumerRegistration registration) {
		if (!(registration instanceof ManagedOpcuaConsumerRegistration)) {
			throw new IllegalArgumentException("Unknown OPC UA consumer registration");
		}
		registration.unregister();
	}

	@Override
	public void onDisconnect(final ConversationContext<OpcuaAPU> context) {
		stopLocalSubscriptions();
		transactions.cancelPending(new PlcConnectionException("OPC UA connection is closing"));
		super.onDisconnect(context);
	}

	@Override
	public void close(final ConversationContext<OpcuaAPU> context) {
		transportClosed();
	}

	/**
	 * 物理连接已关闭时仅回收本地资源，不排队发送不可能成功的关闭报文。
	 */
	void transportClosed() {
		stopLocalSubscriptions();
		transactions.shutdown();
	}

	private void stopLocalSubscriptions() {
		closing = true;
		subscriptions.values().forEach(ManagedOpcuaSubscription::stopLocal);
		subscriptions.clear();
		Plc4xOpcuaAccess.cancelKeepAlive(this);
	}
}
