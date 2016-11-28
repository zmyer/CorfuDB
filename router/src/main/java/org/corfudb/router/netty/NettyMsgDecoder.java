package org.corfudb.router.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.function.Function;

/** A simple decoder which takes netty messages and deserializes them
 * given a decoder function.
 * @param <M> The type of messages the decoder should handle.
 * Created by mwei on 11/25/16.
 */
public class NettyMsgDecoder<M> extends ByteToMessageDecoder {

    /** A function that describes how to convert a bytebuf into messages. */
    private final Function<ByteBuf, M> deserializerFunction;

    /** Get a new decoder with the given deserializer function.
     * @param deserializer  The function to use for deserialization.
     */
    public NettyMsgDecoder(final Function<ByteBuf, M> deserializer) {
        this.deserializerFunction = deserializer;
    }

    /** {@inheritDoc} */
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext,
                          final ByteBuf byteBuf,
                          final List<Object> list) throws Exception {
        list.add(deserializerFunction.apply(byteBuf));
    }

    /** {@inheritDoc} */
    @Override
    protected void decodeLast(final ChannelHandlerContext ctx,
                              final ByteBuf in,
                              final List<Object> out) throws Exception {
        if (in != Unpooled.EMPTY_BUFFER) {
            this.decode(ctx, in, out);
        }
    }
}
