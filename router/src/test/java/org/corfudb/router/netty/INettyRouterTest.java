package org.corfudb.router.netty;

import io.netty.buffer.ByteBuf;
import org.corfudb.router.*;
import org.corfudb.router.pingTest.PingMsg;
import org.corfudb.router.pingTest.PingMsgType;
import org.corfudb.router.test.TestClientRouter;
import org.corfudb.router.test.TestServerRouter;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Created by mwei on 12/6/16.
 */
public interface INettyRouterTest<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType<M>>
{
    static Integer findRandomOpenPort() {
        try (
                ServerSocket socket = new ServerSocket(0);
        ) {
            return socket.getLocalPort();
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        }
    }

    default IServerRouter<M,T> getNewServerRouter(Function<ByteBuf, M> decoderFunction) {
        return NettyServerRouter.<M,T>builder()
                .setPort(findRandomOpenPort())
                .setDecoderSupplier(() -> new NettyMsgDecoder<>(decoderFunction))
                .setEncoderSupplier(NettyMsgEncoder::new)
                .build();
    }

    default IRequestClientRouter<M,T>
        getNewClientRouter(IServerRouter<M,T> serverRouter, Consumer<IClientRouterBuilder<M,T>> clientBuilderFn,
                           Function<ByteBuf, M> decoderFunction) {
        NettyServerRouter<M,T> nettyServerRouter = (NettyServerRouter<M,T>) serverRouter;
        NettyClientRouter.Builder<M,T> clientBuilder = NettyClientRouter.<M,T>builder()
                .setPort((serverRouter == null ? 0 : nettyServerRouter.getPort()))
                .setHost("localhost")
                .setDecoderSupplier(() -> new NettyMsgDecoder<>(decoderFunction))
                .setEncoderSupplier(NettyMsgEncoder::new);
        clientBuilderFn.accept(clientBuilder);
        return clientBuilder.build();
    }

}
