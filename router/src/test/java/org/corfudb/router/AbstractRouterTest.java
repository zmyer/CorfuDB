package org.corfudb.router;

import org.corfudb.AbstractCorfuTest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * Created by mwei on 12/6/16.
 */
public abstract class AbstractRouterTest<M extends IRoutableMsg<T> & IRespondableMsg, T extends IRespondableMsgType<M>>
        extends AbstractCorfuTest {

    final Map<Integer, IServerRouter<M,T>> serverRouterMap = new ConcurrentHashMap<>();
    final Map<Integer, IRequestClientRouter<M,T>> clientRouterMap = new ConcurrentHashMap<>();

    public IServerRouter<M, T> getServerRouter(int routerNumber) {
        return serverRouterMap.computeIfAbsent(routerNumber, num -> getNewServerRouter());
    }

    public abstract IServerRouter<M, T> getNewServerRouter();

    public IRequestClientRouter<M, T> getClientRouter(int routerNumber) {
        return getClientRouter(routerNumber, b -> {});
    }

    public IRequestClientRouter<M, T> getClientRouter(int routerNumber,
                                                      Consumer<IClientRouterBuilder<M,T>> clientBuilder) {
        return clientRouterMap.computeIfAbsent(routerNumber, num -> getNewClientRouter(
                serverRouterMap.get(routerNumber), clientBuilder));
    }

    public abstract IRequestClientRouter<M, T> getNewClientRouter(IServerRouter<M,T> serverRouter,
                                                                  Consumer<IClientRouterBuilder<M,T>> clientBuilder);
}
