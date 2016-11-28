package org.corfudb.router.test;

import org.corfudb.router.IChannel;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.router.IRoutableMsg;

/**
 * Created by mwei on 12/6/16.
 */
public class TestClientToServerChannel <M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType<M>> implements IChannel<M> {

    private final TestServerRouter<M,T> testServerEndpoint;
    private final IChannel<M> serverToClientChannel;

    public TestClientToServerChannel(TestServerRouter<M,T> testServerEndpoint,
                                     IChannel<M> serverToClientChannel) {
        this.testServerEndpoint = testServerEndpoint;
        this.serverToClientChannel = serverToClientChannel;
    }

    @Override
    public void sendMessage(M message) {
        testServerEndpoint.handleMessage(serverToClientChannel, message);
    }
}
