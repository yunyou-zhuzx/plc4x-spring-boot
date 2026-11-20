package com.yunyoucloud.plc4x.client;

import org.apache.plc4x.java.api.authentication.PlcAuthentication;
import org.apache.plc4x.java.opcua.config.OpcuaConfiguration;
import org.apache.plc4x.java.opcua.context.Conversation;
import org.apache.plc4x.java.opcua.context.OpcuaDriverContext;
import org.apache.plc4x.java.opcua.context.SecureChannel;
import org.apache.plc4x.java.opcua.readwrite.*;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 复用上游公开的握手、认证和令牌编码，仅替换保活调度的生命周期。
 */
final class ManagedOpcuaSecureChannel extends SecureChannel {
	private static final ScheduledThreadPoolExecutor TIMER = new ScheduledThreadPoolExecutor(1, runnable -> {
		final Thread thread = new Thread(runnable, "plc-opcua-renew");
		thread.setDaemon(true);
		return thread;
	});

	static {
		TIMER.setRemoveOnCancelPolicy(true);
		TIMER.setKeepAliveTime(1, TimeUnit.SECONDS);
		TIMER.allowCoreThreadTimeOut(true);
	}

	private final Conversation conversation;
	private final ManagedRequestTransactionManager transactions;
	private final long timeoutMillis;
	private final Object lock = new Object();
	private ScheduledFuture<?> renewal;
	private CompletableFuture<Void> pending;
	private long lifetime;
	private boolean stopped;

	ManagedOpcuaSecureChannel(final Conversation conversation, final ManagedRequestTransactionManager transactions,
							  final OpcuaDriverContext context, final OpcuaConfiguration configuration,
							  final PlcAuthentication authentication) {
		super(conversation, transactions, context, configuration, authentication);
		this.conversation = conversation;
		this.transactions = transactions;
		timeoutMillis = configuration.getNegotiationTimeout();
	}

	@Override
	public CompletableFuture<ActivateSessionResponse> onConnect() {
		return conversation.requestHello()
				.thenCompose(ignored -> onConnectOpenSecureChannel(SecurityTokenRequestType.securityTokenRequestTypeIssue, 0, 0))
				.thenCompose(ignored -> onConnectCreateSessionRequest())
				.thenCompose(this::activate)
				.thenApply(response -> {
					scheduleRenewal();
					return response;
				});
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<ActivateSessionResponse> activate(final CreateSessionResponse response) {
		final java.lang.reflect.Method method = org.springframework.util.ReflectionUtils.findMethod(
				SecureChannel.class, "onConnectActivateSessionRequest", CreateSessionResponse.class);
		if (Objects.isNull(method)) {
			return CompletableFuture.failedFuture(new IllegalStateException("Unsupported PLC4X activation implementation"));
		}
		org.springframework.util.ReflectionUtils.makeAccessible(method);
		return (CompletableFuture<ActivateSessionResponse>) org.springframework.util.ReflectionUtils.invokeMethod(method, this, response);
	}

	@Override
	public CompletableFuture<OpenSecureChannelResponse> onConnectOpenSecureChannel(final SecurityTokenRequestType type,
																				   final int channelId, final int requestId) {
		return super.onConnectOpenSecureChannel(type, channelId, requestId).thenApply(response -> {
			synchronized (lock) {
				lifetime = ((ChannelSecurityToken) response.getSecurityToken()).getRevisedLifetime();
				if (lifetime <= 0) {
					throw new IllegalStateException("OPC UA server returned an invalid token lifetime");
				}
			}
			return response;
		});
	}

	private void scheduleRenewal() {
		synchronized (lock) {
			if (!stopped) {
				renewal = TIMER.schedule(this::renew, Math.max(1, (long) Math.ceil(lifetime * 0.75d)), TimeUnit.MILLISECONDS);
			}
		}
	}

	private void renew() {
		synchronized (lock) {
			if (stopped) {
				return;
			}
			pending = transactions.trackOperation(() -> {
				final var transaction = transactions.startRequest();
				transaction.submit(() -> onConnectOpenSecureChannel(SecurityTokenRequestType.securityTokenRequestTypeRenew,
						conversation.getSecurityChannelId(), conversation.getRequestId()).whenComplete((response, failure) -> {
					if (Objects.isNull(failure)) {
						transaction.endRequest();
					} else {
						transaction.failRequest(failure);
					}
				}));
			}, timeoutMillis);
			pending.whenComplete((ignored, failure) -> scheduleRenewal());
		}
	}

	/**
	 * 阻止晚到握手或续期响应再次安排任务，不影响其他连接及上游共享执行器。
	 */
	void stopLocal() {
		synchronized (lock) {
			stopped = true;
			if (Objects.nonNull(renewal)) {
				renewal.cancel(false);
				renewal = null;
			}
			if (Objects.nonNull(pending)) {
				pending.cancel(false);
				pending = null;
			}
		}
	}

	@Override
	public void onDisconnect() {
		stopLocal();
		super.onDisconnect();
	}
}
