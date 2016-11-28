package org.corfudb.router.netty;

import com.google.common.reflect.TypeToken;
import org.corfudb.router.IClientRouter;
import org.corfudb.router.IClientRouterBuilder;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.pingTest.*;
import org.corfudb.router.test.ITestRouterTest;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/6/16.
 */
public class NettyPingTest extends AbstractPingTest implements INettyRouterTest<PingMsg, PingMsgType> {

    @Override
    public IServerRouter<PingMsg, PingMsgType> getNewServerRouter() {
        return INettyRouterTest.super.getNewServerRouter(PingMsg::decode);
    }

    @Override
    public IRequestClientRouter<PingMsg, PingMsgType>
    getNewClientRouter(IServerRouter<PingMsg, PingMsgType> serverRouter,
                       Consumer<IClientRouterBuilder<PingMsg, PingMsgType>> clientBuilder) {
        return INettyRouterTest.super.getNewClientRouter(serverRouter, clientBuilder, PingMsg::decode);
    }


    @Test
    public void messagesSentAfterDisconnectAreResent() throws Exception {
        // Get a server router
        try (IServerRouter<PingMsg, PingMsgType> serverRouter =
                     this.getServerRouter(0)
                             .registerServer(PingServer::new)
                             .start()) {
            // A completable future we will use to monitor disconnection.
            CompletableFuture<Boolean> disconnectFuture = new CompletableFuture<>();
            // Get a client router
            try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                         this.getClientRouter(0, b ->
                                 b.getBuilderAs(new TypeToken<NettyClientRouter.Builder<PingMsg, PingMsgType>>() {})
                                         .setDisconnectFunction(r -> disconnectFuture.complete(true))
                                         .setDefaultTimeout(Duration.of(1, SECONDS)))
                                 .registerRequestClient(PingClient::new)
                                 .start())
            {
                // Stop the server after a connection
                serverRouter.stop();
                // Wait for the client to become disconnected.
                disconnectFuture.get();
                // Try to ping, asynchronously.
                CompletableFuture<Boolean> pingResult =
                        clientRouter.getClient(PingClient.class).ping();
                // Restart the server
                serverRouter.start();
                // The ping should be successful
                assertThat(pingResult.get())
                        .isTrue();
            }
        }
    }
}
