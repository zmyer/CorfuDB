package org.corfudb.router.test;

import lombok.Setter;
import lombok.experimental.Accessors;
import org.corfudb.exceptions.DisconnectedException;
import org.corfudb.router.*;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Created by mwei on 12/6/16.
 */
public class TestClientRouter<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType<M>>
        extends AbstractClientRouter<M,T>
{
    /** The test server which represents the "endpoint" for this client router. */
    final private TestServerRouter<M,T> endpoint;

    /** A channel from the test server to this test client. */
    final private IChannel<M> testServerToClientChannel;

    /** A channel from the client to the test server endpoint. */
    final private IChannel<M> testClientToServerChannel;

    final private boolean automaticallyReconnect;

    volatile boolean connected;

    @Accessors(chain=true)
    public static class Builder<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType<M>>
    implements IClientRouterBuilder<M,T> {

        @Setter
        Duration defaultTimeout = Duration.of(1, SECONDS);

        @Setter
        boolean automaticallyReconnect = true;

        @Setter
        TestServerRouter<M,T> endpoint = null;

        @Override
        public TestClientRouter<M, T> build() {
            return new TestClientRouter<>(defaultTimeout, endpoint, automaticallyReconnect);
        }
    }

    public static <M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType<M>>
    TestClientRouter.Builder<M,T> builder() {
        return new Builder<>();
    }

    private TestClientRouter(Duration defaultTimeout, TestServerRouter<M,T> endpoint, boolean automaticallyReconnect) {
        super(defaultTimeout);
        this.endpoint = endpoint;
        this.testServerToClientChannel = new TestServerToClientChannel<>(this);
        this.testClientToServerChannel = new TestClientToServerChannel<>(endpoint, testServerToClientChannel);
        this.connected = false;
        this.automaticallyReconnect = automaticallyReconnect;
    }

    /** Handle a message from the server.
     *
     * @param msg   The message to handle from the server.
     */
    public void handleMessage(M msg) {
        handleServerMessage(msg, testClientToServerChannel);
    }

    /**
     * Asynchronously send a message to the server.
     *
     * @param msg   The message to send to the server.
     */
    @Override
    public void sendMessage(M msg) {
        if (endpoint == null || !connected && !automaticallyReconnect)
            throw new DisconnectedException("No endpoint connected to this test client router!");

        endpoint.handleMessage(testServerToClientChannel, msg);
    }

    /**
     * Connects to a server endpoint and starts routing client
     * requests, as well as server messages to clients.
     */
    @Override
    public TestClientRouter<M, T> start() {
        connected = true;
        return this;
    }

    /**
     * Disconnects from a server endpoint and stops routing client
     * requests, as well as server messages to clients.
     */
    @Override
    public TestClientRouter<M, T> stop() {
        connected = false;
        return this;
    }
}
