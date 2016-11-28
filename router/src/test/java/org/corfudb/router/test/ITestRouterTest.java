package org.corfudb.router.test;

import org.corfudb.router.*;
import org.corfudb.router.pingTest.PingMsg;
import org.corfudb.router.pingTest.PingMsgType;

import java.time.Duration;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Created by mwei on 12/6/16.
 */
public interface ITestRouterTest<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType<M>> {

    default IServerRouter<M,T> getNewServerRouter() {
        return new TestServerRouter<>();
    }

    default IRequestClientRouter<M,T> getNewClientRouter(IServerRouter<M,T> serverRouter,
                                                         Consumer<IClientRouterBuilder<M,T>> clientBuilder) {
        TestClientRouter.Builder<M,T> builder = TestClientRouter.builder();
        builder.setEndpoint((TestServerRouter<M,T>)serverRouter);
        clientBuilder.accept(builder);
        return builder.build();
    }

}
