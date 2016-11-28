package org.corfudb.router.multiServiceTest;

import org.corfudb.router.AbstractRouterTest;
import org.corfudb.router.IClientRouter;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.netty.NettyClientRouter;
import org.corfudb.router.netty.NettyMsgDecoder;
import org.corfudb.router.netty.NettyMsgEncoder;
import org.corfudb.router.netty.NettyServerRouter;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/6/16.
 */
public abstract class AbstractMultiServiceTest extends AbstractRouterTest<MultiServiceMsg<?>, MultiServiceMsgType> {

    @Test
    public void multipleServicesCorrectlyRoute() throws Exception {
        // Get a server router
        try (IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> serverRouter =
                     this.getServerRouter(0)
                             .registerServer(EchoServer::new)
                             .registerServer(DiscardServer::new)
                             .registerServer(x -> new GatewayServer(x, "open sesame"))
                             .registerServer(x -> new GatedServer(x, x.getServer(GatewayServer.class)))
                             .start()) {
            // Get a client router
            try (IClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> clientRouter =
                         this.getClientRouter(0)
                                 .registerRequestClient(EchoClient::new)
                                 .registerRequestClient(DiscardClient::new)
                                 .registerRequestClient(GatewayClient::new)
                                 .registerRequestClient(x -> new GatedClient(x, x.getClient(GatewayClient.class)))
                                 .start()) {

                // Test if we get an echo back from the echo sever.
                assertThat(clientRouter.getClient(EchoClient.class).echo("hello world!").get())
                        .isEqualTo("hello world!");

                // Make sure we can discard a message.
                clientRouter.getClient(DiscardClient.class).discard("abc");
                assertThat(serverRouter.getServer(DiscardServer.class).getDiscarded().get())
                        .isTrue();

                // And check if we can get a "gated" echo
                assertThat(clientRouter.getClient(GatedClient.class).gatedEcho("hello world!").get())
                        .isEqualTo("hello world!");
            }
        }
    }


    @Test
    public void preconditionFailureTest() throws Exception {
        // A gateway server which will provide the secret to the gated server
        GatewayServer gs = new GatewayServer(null, "open sesame");
        // Get a server router
        try (IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> serverRouter =
                     this.getServerRouter(0)
                             .registerServer(x -> new GatewayServer(x, "wrong password")) //serve the wrong password.
                             .registerServer(x -> new GatedServer(x, gs)) //get the password from the other gs
                             .start()) {
            // Get a client router
            try (IClientRouter<MultiServiceMsg<?>, MultiServiceMsgType> clientRouter =
                         this.getClientRouter(0)
                                 .registerRequestClient(GatewayClient::new)
                                 .registerRequestClient(x -> new GatedClient(x, x.getClient(GatewayClient.class)))
                                 .start()) {

                // And check if we can get a "gated" echo
                assertThatThrownBy(() -> clientRouter.getClient(GatedClient.class).gatedEcho("hello world!").get())
                        .hasCauseInstanceOf(WrongPasswordException.class);
            }
        }
    }
}
