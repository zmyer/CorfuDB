package org.corfudb.router.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.exceptions.DisconnectedException;
import org.corfudb.exceptions.NetworkException;
import org.corfudb.router.AbstractClientRouter;
import org.corfudb.router.IChannel;
import org.corfudb.router.IClient;
import org.corfudb.router.IClientRouter;
import org.corfudb.router.IClientRouterBuilder;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.router.IRoutableMsg;
import org.corfudb.router.NamedThreadFactory;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.SECONDS;

/** A implementation of the client router using Netty.
 * Supports requests and automatic reconnection.
 * @param <M> The type of messages this client router should route.
 * @param <T> The type of the message type.
 * Created by mwei on 11/23/16.
 */
@Slf4j
@ChannelHandler.Sharable
public class NettyClientRouter<M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType<M>>
        extends AbstractClientRouter<M, T> {

    /** The number of bytes to use to space frames. */
    private static final int MAX_FRAME_BYTES = 4;

    /**
     * The worker group for this router.
     */
    private EventLoopGroup workerGroup;

    /**
     * The event executor group for this router.
     */
    private EventExecutorGroup ee;

    /**
     * The currently registered channel.
     */
    private Channel channel;

    /**
     * The currently registered handler for the channel.
     */
    private NettyClientChannelHandler channelHandler;

    /**
     * Whether or not this client has been started or is stopped.
     */
    private volatile boolean started = false;

    /** A queue of messages queued during a reconnect.
     *
     */
    private final Queue<M> queuedMessages = new ConcurrentLinkedQueue<>();

    /**
     * The hostname of the server we should connect to.
     */
    @Getter
    private final String host;

    /**
     * The port of the server we should connect to.
     */
    @Getter
    private final int port;

    /** The message decoder. */
    private final Supplier<ChannelHandlerAdapter> decoderSupplier;

    /** The message encoder. */
    private final Supplier<ChannelHandlerAdapter> encoderSupplier;

    /** Whether or not to automatically reconnect on failed connection. */
    private final boolean automaticallyReconnect;

    /** A function to execute before reconnecting which gets this router and
     *  returns true to continue reconnecting, or false to shutdown.
     */
    private final Function<NettyClientRouter<M, T>, Boolean> reconnectFunction;

    /** Whether or not to queue messages on a failed connection, or to
     * immediately throw a disconnected exception. Note that this does not
     * affect messages in flight - any messages which have already been
     * sent but not responded to will throw a disconnected exception
     * because we will obtain a different channel.
     */
    private final boolean queueMessagesOnFailure;

    /** A function which executes whenever this router gets
     * disconnected unexpectedly.
     */
    private final Consumer<IRequestClientRouter<M, T>> disconnectedFunction;

    /** Create a new NettyClientRouter with the given arguments.
     * @param builder   The builder to use to build this client.
     */
    protected NettyClientRouter(final Builder<M, T> builder) {
        super(builder.defaultTimeout);
        this.host = builder.getHost();
        this.port = builder.getPort();
        this.decoderSupplier = builder.getDecoderSupplier();
        this.encoderSupplier = builder.getEncoderSupplier();
        this.automaticallyReconnect = builder.isAutomaticallyReconnect();
        this.reconnectFunction = builder.getReconnectFunction();
        this.queueMessagesOnFailure = builder.isQueueMessageOnFailure();
        this.disconnectedFunction = builder.getDisconnectFunction();
    }

    /** This builder class sets defaults for the client.
     * @param <M> The type of messages the router accepts.
     * @param <T> The type of the message type. */
    @Accessors(chain = true)
    @Data
    public static class Builder<M extends IRoutableMsg<T> & IRespondableMsg,
            T extends IRespondableMsgType<M>>
        implements IClientRouterBuilder<M, T> {
        /** The default timeout if not set by the client. */
        static final Duration DEFAULT_TIMEOUT = Duration.of(5, SECONDS);

        /** The default retry divider, which is what fraction of the timeout
         * to retry.
         */
        static final int DEFAULT_TIMEOUT_RETRY_DIVIDER = 4;

        /** The endpoint to use, in host:port format. */
        private String endpoint;

        /** The host to use as an endpoint. */
        private String host;

        /** The port to use as an endpoint. */
        private int port;

        /** A factory to obtain decoders from messages from Netty buffers. */
        private Supplier<ChannelHandlerAdapter> decoderSupplier;

        /** A factory to obtain encoders from messages to Netty buffers. */
        private Supplier<ChannelHandlerAdapter> encoderSupplier;

        /** How long to wait before timing out requests. */
        private Duration defaultTimeout = DEFAULT_TIMEOUT;

        /** Whether or not to automatically reconnect. */
        private boolean automaticallyReconnect = true;

        /** A function that is executed when the client needs to reconnect.
         * Returning false in the function aborts the reconnection process.
         * By default, we wait one fourth default timeout time to reconnect,
         * and try forever. This ensures that we try to reconnect before
         * any futures timeout.
         * */
        private Function<NettyClientRouter<M, T>, Boolean>
                reconnectFunction = (r) -> {
            try {
                Thread.sleep(defaultTimeout.toMillis()
                        / DEFAULT_TIMEOUT_RETRY_DIVIDER);
            } catch (InterruptedException ie) {
                return false; // abort reconnecting if interrupted
            }
            return true;
        };

        /** Whether or not to queue messages on failure, or to just
         * reject them with a disconnected exception.
         */
        private boolean queueMessageOnFailure = true;

        /** The disconnect function is executed when the client is
         * disconnected.
         */
        private Consumer<IRequestClientRouter<M, T>> disconnectFunction
                = r -> { };

        /** Build a Netty client router with the given parameters.
         * @return A new Netty client instance with the given parameters. */
        public NettyClientRouter<M, T> build() {
            if (endpoint != null) {
                host = endpoint.split(":")[0];
                port = Integer.parseInt(endpoint.split(":")[1]);
            }
            return new NettyClientRouter<>(this);
        }
    }

    /** Obtain a builder to build client routers.
     * @param <M>   The type of the message the client router should process.
     * @param <T>   The type of the message type.
     * @return      A builder to build a client router.
     */
    public static <M extends IRoutableMsg<T> & IRespondableMsg,
            T extends IRespondableMsgType<M>> Builder<M, T>
        builder() {
            return new Builder<>(); }


    /**
     * {@inheritDoc}.
     */
    @Override
    public NettyClientRouter<M, T> registerClient(final IClient<M, T> client) {
        super.registerClient(client);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessage(final M msg) {
        if (channel != null && channel.isActive()) {
            try {
                channel.writeAndFlush(msg);
            } catch (Exception e) {
                if (automaticallyReconnect && queueMessagesOnFailure) {
                    queuedMessages.add(msg);
                    return;
                }
                throw new NetworkException(getEndpoint(), e);
            }
        } else {
            if (started && automaticallyReconnect && queueMessagesOnFailure) {
                queuedMessages.add(msg);
                return;
            }
            throw new DisconnectedException(getEndpoint());
        }
    }

    /**
     * Connects to a server endpoint and starts routing client
     * requests, as well as server messages to clients.
     */
    @Override
    public synchronized IClientRouter<M, T> start() {
        // If the worker group and the event group already exist, try to
        // reuse them, otherwise, create new pools.

        workerGroup =
            workerGroup != null && !workerGroup.isShutdown() ? workerGroup
                    : new NioEventLoopGroup(Runtime.getRuntime()
                    .availableProcessors() * 2,
            new NamedThreadFactory("io"));

        ee =
            ee != null && !ee.isShutdown() ? ee
                    : new DefaultEventExecutorGroup(
                            Runtime.getRuntime()
                                    .availableProcessors() * 2,
            new NamedThreadFactory("event"));

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.SO_REUSEADDR, true);
        b.option(ChannelOption.TCP_NODELAY, true);

        final NettyClientChannelHandler handler = new
                NettyClientChannelHandler();
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(final SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldPrepender(
                        MAX_FRAME_BYTES));
                ch.pipeline().addLast(
                        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                                0,
                                MAX_FRAME_BYTES,
                                0,
                                MAX_FRAME_BYTES));
                ch.pipeline().addLast(ee, decoderSupplier.get());
                ch.pipeline().addLast(ee, encoderSupplier.get());
                ch.pipeline().addLast(ee, handler);
            }
        });

        try {
            ChannelFuture cf = b.connect(host, port);
            cf.syncUninterruptibly();
            channel = cf.channel();
            channel.closeFuture().addListener(this::channelClosed);
            channelHandler = handler;
            started = true;
        } catch (Exception e) {
            // No need to change the state of started,
            // If we were previously started, we are still started (retry)
            // If we were not previously started, then we will not start.
            throw new NetworkException(host + ":" + port, e);
        }

        return this;
    }


    /**
     * Disconnects from a server endpoint and stops routing client
     * requests, as well as server messages to clients.
     */
    @Override
    public synchronized IClientRouter<M, T> stop() {
        started = false;

        // Close the channel, if it is open.
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        // Shutdown all worker thread pools
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (ee != null) {
            ee.shutdownGracefully();
        }
        // Wait for the channel to close.
        if (channel != null) {
            try {
                channel.closeFuture().sync();
            } catch (InterruptedException ie) {
                log.warn("Unexpected InterruptedException while shutting "
                        + "down netty client router", ie);
                throw new RuntimeException("Interrupted while trying to "
                       + "shutdown netty client router", ie);
            }
        }
        return this;
    }

    /** Called internally when a channel is closed to cleanup pending
     * requests and do housekeeping.
     * @param channelFuture     The channel future for the channel.
     */
    private void channelClosed(final Future<?> channelFuture) {
        // Cleanup any outstanding messages by causing them to
        // throw a disconnected exception.
        final DisconnectedException de = new
                DisconnectedException(getEndpoint());
        cancelAllPendingRequestsExceptionally(de);

        // If we didn't expect this channel to unregister (the router was not
        // stopped), log a WARN level message
        if (started) {
            log.warn("Unexpected disconnection from endpoint "
                    + getEndpoint() + "!");
            // Fire the disconnected function.
            disconnectedFunction.accept(this);
            // If the client router has been configured to automatically
            // reconnect, handle it.
            if (automaticallyReconnect) {
                while (reconnectFunction.apply(this)) {
                    try {
                        log.info("Trying to reconnect to endpoint "
                                + getEndpoint());
                        start();
                        return;
                    } catch (Exception e) {
                        log.warn("Reconnecting to endpoint "
                                + getEndpoint() + " failed!");
                    }
                }
                // The reconnect function asked us to exit, try stopping to
                // cleanup anything left over from the reconnect loop.
                stop();
            } else {
                stop();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public NettyClientRouter<M, T> registerRequestClient(
            final Function<IRequestClientRouter<M, T>,
                    IClient<M, T>> clientSupplier) {
        super.registerRequestClient(clientSupplier);
        return this;
    }

    /** A channel handler for a inbound netty channel. */
    private class NettyClientChannelHandler
        extends SimpleChannelInboundHandler {

        /** The currently registered channel handler context. */
        private ChannelHandlerContext context;

        /** The wrapped channel for this context. */
        private IChannel<M> channel;

        /**
         * Read a message in the channel.
         *
         * @param channelHandlerContext The channel context the message is
         *                              coming from.
         * @param m                     The message read from the channel.
         * @throws Exception An exception thrown during channel processing.
         */
        @Override
        @SuppressWarnings("unchecked")
        protected void channelRead0(
                final ChannelHandlerContext channelHandlerContext,
                                    final Object m) throws Exception {
            handleServerMessage((M) m, channel);
        }


        /**
         * Called when the channel is registered (i.e., when a remote endpoint
         * connects.  Since only one bootstrap should use this channel,
         * channelUnregistered must be called before this method will be
         * called again.
         *
         * @param ctx The context of the channel that was registered.
         * @throws Exception An exception thrown during the registration of this
         *                   channel.
         */
        @Override
        public void channelRegistered(final ChannelHandlerContext ctx)
                throws Exception {
            super.channelRegistered(ctx);
            log.debug("Registered new channel {}", ctx);
        }

        /**
         * Called when the channel is unregistered (i.e., the remote endpoint
         * disconnects. Since only one bootstrap should use this channel,
         * channelRegistered must be called before this method is called.
         *
         * @param ctx The context for the channel which was disconnected.
         * @throws Exception An exception thrown during the disconnection
         * of the channel.
         */
        @Override
        public void channelUnregistered(final ChannelHandlerContext ctx)
                throws Exception {
            log.debug("Unregistered channel {}", ctx);
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx)
                throws Exception {
            super.channelActive(ctx);
            context = ctx;
            channel = new NettyChannelWrapper<M>(ctx);
            log.debug("Channel {} active", ctx);
            if (queueMessagesOnFailure && queuedMessages.size() > 0) {
                log.info("Resending {} queued messages", queuedMessages.size());
                M msg;
                while ((msg = queuedMessages.poll()) != null) {
                    sendMessage(msg);
                }
            }
        }
    }

    /**
     * Get the name of the endpoint this router is connected to.
     * @return The name of the endpoint the router is connected to.
     */
    private String getEndpoint() {
        return host + ":" + port;
    }
}
