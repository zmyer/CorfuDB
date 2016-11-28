package org.corfudb.router.netty;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.router.IChannel;

/** A wrapper which wraps Netty channels into an IChannel.
 * @param <M> The type of messages to send down this channel.
 * Created by mwei on 11/29/16.
 */
public class NettyChannelWrapper<M> implements IChannel<M> {

    /** The underlying Netty channel handler context. */
    private final ChannelHandlerContext ctx;

    /** Obtain a new Netty channel wrapper using the given handler context.
     * @param context   The Netty channel context
     */
    public NettyChannelWrapper(final ChannelHandlerContext context) {
        this.ctx = context;
    }

    /** {@inheritDoc} */
    @Override
    public void sendMessage(final M message) {
        ctx.writeAndFlush(message);
    }
}
