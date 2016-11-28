package org.corfudb.router.netty;

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
public class NettyMultiServiceTest extends AbstractMultiServiceTest
        implements INettyRouterTest<MultiServiceMsg<?>, MultiServiceMsgType> {

    @Override
    public IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> getNewServerRouter() {
        return INettyRouterTest.super.getNewServerRouter(MultiServiceMsg::decode);
    }

    @Override
    public IRequestClientRouter<MultiServiceMsg<?>, MultiServiceMsgType>
    getNewClientRouter(IServerRouter<MultiServiceMsg<?>, MultiServiceMsgType> serverRouter,
                       Consumer<IClientRouterBuilder<MultiServiceMsg<?>, MultiServiceMsgType>> clientBuilder) {
        return INettyRouterTest.super.getNewClientRouter(serverRouter, clientBuilder, MultiServiceMsg::decode);
    }

}
