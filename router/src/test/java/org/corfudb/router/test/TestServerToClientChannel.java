package org.corfudb.router.test;

import org.corfudb.router.IChannel;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.router.IRoutableMsg;

/**
 * Created by mwei on 12/6/16.
 */
public class TestServerToClientChannel<M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType<M>> implements IChannel<M> {

    private final TestClientRouter<M,T> testClientEndpoint;

    public TestServerToClientChannel(TestClientRouter<M,T> testClientEndpoint) {
        this.testClientEndpoint = testClientEndpoint;
    }

    @Override
    public void sendMessage(M message) {
        testClientEndpoint.handleMessage(message);
    }
}
