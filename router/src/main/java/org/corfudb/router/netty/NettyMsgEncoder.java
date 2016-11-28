package org.corfudb.router.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/** A simple encoder which takes messages which are encodeable into Netty
 * messages and seriazlies them into Netty bytebufs.
 * @param <M> The type of message this encoder should handle.
 * Created by mwei on 11/25/16.
 */
@Slf4j
public class NettyMsgEncoder<M extends INettyEncodableMsg>
        extends MessageToByteEncoder {

    /** {@inheritDoc} */
    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext,
                          final Object msg,
                          final ByteBuf byteBuf) throws Exception {
        try {
            ((M) msg).encode(byteBuf);
        } catch (Exception e) {
            log.error("Error during serialization!", e);
        }
    }
}
