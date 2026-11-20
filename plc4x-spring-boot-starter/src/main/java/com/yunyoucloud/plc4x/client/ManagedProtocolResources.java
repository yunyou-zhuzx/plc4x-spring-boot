package com.yunyoucloud.plc4x.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.apache.plc4x.java.spi.Plc4xNettyWrapper;
import org.apache.plc4x.java.spi.TimedOperation;
import org.apache.plc4x.java.spi.TimeoutManager;
import org.apache.plc4x.java.spi.internal.HandlerRegistration;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 适配 PLC4X 0.13.1 的响应注册表，让取消、异常和远端断线也能释放报文等待器。
 */
final class ManagedProtocolResources implements TimeoutManager {

	private static final Field HANDLERS = requiredField("registeredHandlers");
	private static final Field TIMEOUTS = requiredField("timeoutManager");
	private static final ScheduledThreadPoolExecutor TIMER = new ScheduledThreadPoolExecutor(1, runnable -> {
		final Thread thread = new Thread(runnable, "plc-response-timeout");
		thread.setDaemon(true);
		return thread;
	});

	static {
		TIMER.setRemoveOnCancelPolicy(true);
		TIMER.setKeepAliveTime(1, TimeUnit.SECONDS);
		TIMER.allowCoreThreadTimeOut(true);
	}

	private final Set<Pending> pending = new HashSet<>();
	private final ConcurrentHashMap<Thread, Pending> registering = new ConcurrentHashMap<>();
	private final Registry handlers = new Registry();
	private boolean stopped;

	/**
	 * 在握手启动前替换每连接计时器，保留上游协议编码及响应匹配逻辑。
	 */
	static ManagedProtocolResources install(final ChannelPipeline pipeline) {
		final Plc4xNettyWrapper<?> wrapper = pipeline.get(Plc4xNettyWrapper.class);
		if (Objects.isNull(wrapper)) {
			throw new IllegalStateException("Unsupported PLC4X protocol pipeline");
		}
		final ManagedProtocolResources resources = new ManagedProtocolResources();
		((TimeoutManager) ReflectionUtils.getField(TIMEOUTS, wrapper)).stop();
		ReflectionUtils.setField(HANDLERS, wrapper, resources.handlers);
		ReflectionUtils.setField(TIMEOUTS, wrapper, resources);
		pipeline.addLast(new ChannelInboundHandlerAdapter() {
			@Override
			public void channelInactive(final ChannelHandlerContext context) throws Exception {
				resources.stop();
				super.channelInactive(context);
			}
		});
		return resources;
	}

	@Override
	public CompletionCallback<?> register(final TimedOperation operation) {
		final Pending waiting = new Pending(operation);
		synchronized (pending) {
			if (stopped || pending.size() >= 64) {
				throw new RejectedExecutionException("PLC wire response registry is closed or full");
			}
			pending.add(waiting);
		}
		registering.put(Thread.currentThread(), waiting);
		return waiting;
	}

	@Override
	public void stop() {
		final Set<Pending> snapshot;
		synchronized (pending) {
			stopped = true;
			snapshot = new HashSet<>(pending);
		}
		snapshot.forEach(Pending::abort);
		registering.clear();
		handlers.clear();
	}

	int pendingCount() {
		synchronized (pending) {
			return pending.size();
		}
	}

	private static Field requiredField(final String name) {
		final Field field = ReflectionUtils.findField(Plc4xNettyWrapper.class, name);
		if (Objects.isNull(field)) {
			throw new IllegalStateException("Unsupported PLC4X response registry: " + name);
		}
		ReflectionUtils.makeAccessible(field);
		return field;
	}

	/**
	 * 上游完成注册后才绑定事务，避免超时回调看到尚未公布的响应对象。
	 */
	private final class Registry extends ConcurrentLinkedQueue<HandlerRegistration> {
		@Override
		public boolean add(final HandlerRegistration handler) {
			final Pending waiting = registering.remove(Thread.currentThread());
			if (Objects.isNull(waiting)) {
				throw new IllegalStateException("PLC response registered without a timeout");
			}
			final HandlerRegistration contextual = new HandlerRegistration(handler.getName(), handler.getCommands(),
					handler.getExpectClazz(), ManagedRequestTransactionManager.bindCurrentCallback((java.util.function.Consumer<Object>) handler.getPacketConsumer()),
					handler.getOnTimeoutConsumer(), handler.getErrorConsumer(), handler::confirmHandled, handler::confirmError,
					handler::cancel, handler.getTimeout());
			final boolean added = super.add(contextual);
			waiting.bind(contextual);
			return added;
		}
	}

	/**
	 * 本地终态先解除引用，再取消计时器；不在资源锁中执行协议回调。
	 */
	private final class Pending implements CompletionCallback<Object> {
		private final TimedOperation operation;
		private HandlerRegistration registration;
		private ScheduledFuture<?> timer;
		private Runnable unbind = () -> {
		};
		private boolean completed;

		private Pending(final TimedOperation operation) {
			this.operation = operation;
		}

		private void bind(final HandlerRegistration handler) {
			synchronized (this) {
				registration = handler;
				if (completed) {
					handlers.remove(handler);
					handler.cancel();
					return;
				}
				timer = TIMER.schedule(this::expire, Math.max(1, operation.getTimeout().toMillis()), TimeUnit.MILLISECONDS);
			}
			final Runnable release = ManagedRequestTransactionManager.onCurrentCompletion(this::abort);
			synchronized (this) {
				if (!completed) {
					unbind = release;
					return;
				}
			}
			release.run();
		}

		@Override
		public void complete() {
			finish(false);
		}

		private void abort() {
			finish(true);
		}

		private void expire() {
			if (finish(true)) {
				operation.getOnTimeoutConsumer().accept(new TimeoutException("PLC wire response timed out"));
			}
		}

		private boolean finish(final boolean cancel) {
			final Runnable release;
			synchronized (this) {
				if (completed) {
					return false;
				}
				completed = true;
				if (Objects.nonNull(timer)) {
					timer.cancel(false);
				}
				if (Objects.nonNull(registration)) {
					handlers.remove(registration);
					if (cancel) {
						registration.cancel();
					}
				}
				release = unbind;
				unbind = () -> {
				};
			}
			synchronized (pending) {
				pending.remove(this);
			}
			release.run();
			return true;
		}
	}
}
