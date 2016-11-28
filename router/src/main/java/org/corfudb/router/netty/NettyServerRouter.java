package org.corfudb.router.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.router.AbstractServerRouter;
import org.corfudb.router.IRoutableMsg;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.NamedThreadFactory;

import java.util.function.Supplier;

/** An implementation of the server router using Netty.
 * @param <M> The type of the messages this server router may handle.
 * @param <T> The type of the message type.
 * Created by mwei on 11/23/16.
 */
@Slf4j
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class NettyServerRouter<M extends IRoutableMsg<T>, T>
        extends AbstractServerRouter<M, T> {

    /** The number of bytes to use to space frames. */
    private static final int MAX_FRAME_BYTES = 4;

    /** The number of backlogged messages. */
    private static final int SO_MAXBACKLOG = 100;

    /** The boss group services incoming connections. */
    private EventLoopGroup bossGroup;

    /** The worker group performs the actual work after
     * getting an incoming message. */
    private EventLoopGroup workerGroup;

    /** The event group handles actual application level logic. */
    private EventExecutorGroup ee;

    /** The future for the channel the server is listening on. */
    private ChannelFuture channelFuture;

    /** The port to listen to incoming requests on. */
    @Getter
    private final int port;

    /** The message decoder. */
    private final Supplier<ChannelHandlerAdapter> decoderSupplier;

    /** The message encoder. */
    private final Supplier<ChannelHandlerAdapter> encoderSupplier;

    /** The type of channel to use. */
    private final Class<? extends ServerSocketChannel> channelType;

    /** A builder for NettyServerRouters. Sets defaults for the router.
     * @param <M>   The type of messages this server router handles.
     * @param <T>   The type of the message type.
     */
    @Accessors(chain = true)
    @Data
    public static class Builder<M extends IRoutableMsg<T>, T> {

        /** The port this router should service messages on. */
        private int port;

        /** A factory which supplies decoders. */
        private Supplier<ChannelHandlerAdapter> decoderSupplier;

        /** A factory which supplies encoders. */
        private Supplier<ChannelHandlerAdapter> encoderSupplier;

        /** The type of channel to use. */
        private Class<? extends ServerSocketChannel> channelType =
                NioServerSocketChannel.class;

        /** Build a new instance of a NettyServerRouter using
         * the parameters given.
         * @return  A new instance of NettyServerRouter
         */
        public NettyServerRouter<M, T> build() {
            return new NettyServerRouter<>(port, decoderSupplier,
                    encoderSupplier, channelType);
        }
    }

    /** Return a new builder.
     * @param <M>   The type of messages the server router should handle.
     * @param <T>   The type of the message type.
     * @return      A builder for building new instances of the router.
     */
    public static <M extends IRoutableMsg<T>, T> Builder<M, T> builder() {
        return new Builder<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NettyServerRouter<M, T> registerServer(final IServer<M, T>  server) {
        super.registerServer(server);
        return this;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized NettyServerRouter<M, T> start() {
        // Don't allow a server router to be started twice.
        if (channelFuture != null && channelFuture.channel().isOpen()) {
            throw new RuntimeException("Attempted to start a server with "
                   + "an open channel!");
        }

        bossGroup = new NioEventLoopGroup(1,
                new NamedThreadFactory("accept"));

        workerGroup = new NioEventLoopGroup(Runtime.getRuntime()
                .availableProcessors() * 2,
                new NamedThreadFactory("io"));

        ee = new DefaultEventExecutorGroup(Runtime.getRuntime()
                .availableProcessors() * 2,
                new NamedThreadFactory("event"));

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                .channel(channelType)
                .option(ChannelOption.SO_BACKLOG, SO_MAXBACKLOG)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.ALLOCATOR,
                        PooledByteBufAllocator.DEFAULT)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(
                            final io.netty.channel.socket.SocketChannel ch)
                            throws Exception {
                        ch.pipeline().addLast(new
                                LengthFieldPrepender(MAX_FRAME_BYTES));
                        ch.pipeline().addLast(new
                                LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                                0,
                                MAX_FRAME_BYTES,
                                0,
                                MAX_FRAME_BYTES));
                        ch.pipeline().addLast(ee, decoderSupplier.get());
                        ch.pipeline().addLast(ee, encoderSupplier.get());
                        ch.pipeline().addLast(ee,
                                new NettyServerRouterChannel());
                        }
                    });

            channelFuture = b.bind(port).sync();
        }  catch (InterruptedException ie) {
            log.error("Netty server router shut down unexpectedly "
                   + "during startup due to interruption", ie);
            throw new RuntimeException("InterruptionException starting "
                   + "Netty server router", ie);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized IServerRouter<M, T> stop() {
        // Shutdown the channel, if there is one.
        if (channelFuture != null) {
            channelFuture.channel().close();
        }
        // Shutdown the worker threads, if there are any active.
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (ee != null) {
            ee.shutdownGracefully();
        }
        // Wait for the channel to shutdown.
        if (channelFuture != null) {
            try {
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException ie) {
                log.error("Netty server router shut down unexpectedly "
                       + "during shutdown due to interruption", ie);
                throw new RuntimeException("InterruptionException stopping"
                        + " Netty server router", ie);
            }
        }
        return this;
    }

    /** An internal class which implements the actual server channel. */
    private class NettyServerRouterChannel
            extends ChannelInboundHandlerAdapter {

        /** The channel wrapper for this channel. */
        private NettyChannelWrapper<M> channelWrapper;

        /** {@inheritDoc} */
        @Override
        public void channelRegistered(final ChannelHandlerContext ctx) throws
                Exception {
            super.channelRegistered(ctx);
            channelWrapper = new NettyChannelWrapper<M>(ctx);
        }

        /**
         * Handle an incoming message read on the channel.
         *
         * @param ctx Channel handler context
         * @param msg The incoming message on that channel.
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx,
                                final Object msg) {
            try {
                // The incoming message should have been transformed
                // earlier in the pipeline.
                handleMessage(channelWrapper, (M) msg);
            } catch (Exception e) {
                log.error("Exception during read!", e);
            }
        }


        /** Catch an exception handling an inbound message type.
         *
         * @param ctx       The context that encountered the error.
         * @param cause     The reason for the error.
         */
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx,
                                    final Throwable cause) {
            log.error("Error in handling inbound message, {}", cause);
            ctx.close();
        }
    }
}
