package org.corfudb.runtime.clients;

import lombok.Getter;
import org.corfudb.infrastructure.AbstractServerTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.router.IClient;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.test.TestClientRouter;
import org.junit.Before;

import java.util.function.Function;

/**
 * Created by mwei on 12/13/15.
 */
public abstract class AbstractClientTest
        <C extends IClient<CorfuMsg, CorfuMsgType>,
         S extends IServer<CorfuMsg,CorfuMsgType>>
        extends AbstractServerTest<S> {

    /** The test client router. */
    private TestClientRouter<CorfuMsg, CorfuMsgType> clientRouter;

    /** The current client for this test. */
    @Getter
    private C client;

    /** A function which generates clients from routers. */
    private final Function<IRequestClientRouter<CorfuMsg, CorfuMsgType>, C>
                                                                clientGenerator;

    /**
     * Initialize the AbstractClientTest.
     */
    public AbstractClientTest(
        Function<IRequestClientRouter<CorfuMsg, CorfuMsgType>, C> clientFactory,
        Function<IServerRouter<CorfuMsg, CorfuMsgType>, S> serverFactory) {
        super(serverFactory);
        clientGenerator = clientFactory;
    }

    /** Reset the client and the server. */
    @Before
    @Override
    public void resetTest() {
        if (clientRouter != null) {
            clientRouter.stop();
        }
        super.resetTest();
        clientRouter = TestClientRouter.<CorfuMsg, CorfuMsgType>builder()
                .setAutomaticallyReconnect(true)
                .setDefaultTimeout(PARAMETERS.TIMEOUT_NORMAL)
                .setEndpoint(router)
                .build();
        client = clientGenerator.apply(clientRouter);
    }
}
