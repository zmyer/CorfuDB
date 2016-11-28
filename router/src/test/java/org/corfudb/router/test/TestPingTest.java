package org.corfudb.router.test;

import org.corfudb.router.IClientRouterBuilder;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.pingTest.AbstractPingTest;
import org.corfudb.router.pingTest.PingMsg;
import org.corfudb.router.pingTest.PingMsgType;

import java.util.function.Consumer;

/**
 * Created by mwei on 12/6/16.
 */
public class TestPingTest extends AbstractPingTest implements ITestRouterTest<PingMsg, PingMsgType> {

    @Override
    public IServerRouter<PingMsg, PingMsgType> getNewServerRouter() {
        return ITestRouterTest.super.getNewServerRouter();
    }

    @Override
    public IRequestClientRouter<PingMsg, PingMsgType>
        getNewClientRouter(IServerRouter<PingMsg, PingMsgType> serverRouter,
                           Consumer<IClientRouterBuilder<PingMsg,PingMsgType>> clientBuilder) {
        return ITestRouterTest.super.getNewClientRouter(serverRouter, clientBuilder);
    }
}
