package org.corfudb.router.test;

import org.corfudb.router.IClientRouterBuilder;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.multiServiceTest.AbstractMultiServiceTest;
import org.corfudb.router.multiServiceTest.MultiServiceMsg;
import org.corfudb.router.multiServiceTest.MultiServiceMsgType;

import java.util.function.Consumer;

/**
 * Created by mwei on 12/6/16.
 */
public class TestMultiServiceTest extends AbstractMultiServiceTest
        implements ITestRouterTest<MultiServiceMsg<?>, MultiServiceMsgType>  {
    @Override
    public IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> getNewServerRouter() {
        return ITestRouterTest.super.getNewServerRouter();
    }

    @Override
    public IRequestClientRouter<MultiServiceMsg<?>, MultiServiceMsgType>
    getNewClientRouter(IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> serverRouter,
                       Consumer<IClientRouterBuilder<MultiServiceMsg<?>, MultiServiceMsgType>> clientBuilder) {
        return ITestRouterTest.super.getNewClientRouter(serverRouter, clientBuilder);
    }
}
