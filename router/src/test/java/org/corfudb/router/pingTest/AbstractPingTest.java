package org.corfudb.router.pingTest;

import org.corfudb.exceptions.DisconnectedException;
import org.corfudb.router.AbstractRouterTest;
import org.corfudb.router.IClientRouter;
import org.corfudb.router.IServerRouter;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/6/16.
 */
public abstract class AbstractPingTest extends AbstractRouterTest<PingMsg, PingMsgType> {

    @Test
    public void canPing() throws Exception {
        // Get a server router
        try (IServerRouter<PingMsg, PingMsgType> serverRouter =
                     this.getServerRouter(0)
                             .registerServer(PingServer::new)
                             .start()) {
            // Get a client router
            try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                         this.getClientRouter(0)
                                 .registerRequestClient(PingClient::new)
                                 .start()) {
                assertThat(clientRouter.getClient(PingClient.class).ping().get())
                        .isTrue();
            }
        }
    }

    @Test
    public void unconnectedClientThrowsException() throws Exception {
        try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                     this.getClientRouter(0)
                             .registerRequestClient(PingClient::new)) {

            assertThatThrownBy(() -> clientRouter.getClient(PingClient.class).ping().get())
                    .hasCauseInstanceOf(DisconnectedException.class);

        }
    }


    @Test
    public void disconnectedClientThrowsException() throws Exception {
        try (IServerRouter<PingMsg, PingMsgType> serverRouter =
                     this.getServerRouter(0)
                             .registerServer(PingServer::new)
                             .start()) {
            // Get a client router
            try (IClientRouter<PingMsg, PingMsgType> clientRouter =
                         this.getClientRouter(0, b -> b.setAutomaticallyReconnect(false))
                                 .registerRequestClient(PingClient::new)) {
                // Stop the server after a connection
                serverRouter.stop();
                // Get and ping, should throw disconnected exception.
                assertThatThrownBy(() -> clientRouter.getClient(PingClient.class).ping().get())
                        .hasCauseInstanceOf(DisconnectedException.class);
            }
        }
    }


}
