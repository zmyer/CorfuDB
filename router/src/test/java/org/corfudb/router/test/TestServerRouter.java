package org.corfudb.router.test;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.router.*;

import java.util.function.Function;

/**
 * Created by mwei on 11/29/16.
 */
public class TestServerRouter<M extends IRoutableMsg<T>, T>
    extends AbstractServerRouter<M,T> {


    public TestServerRouter() {

    }

    /**
     * Register a new server to route messages to, given
     * a function which generates a server from this router.
     *
     * @param serverSupplier The function which supplies the server.
     * @return This server router, to support chaining.
     */
    @Override
    public TestServerRouter<M, T> registerServer(Function<IServerRouter<M, T>, IServer<M, T>> serverSupplier) {
        super.registerServer(serverSupplier);
        return this;
    }

    /**
     * Register a new server to route messages to.
     *
     * @param server The server to route messages to.
     * @return This server router, to support chaining.
     */
    @Override
    public TestServerRouter<M, T> registerServer(IServer<M, T> server) {
        super.registerServer(server);
        return this;
    }

    @Override
    public TestServerRouter<M, T> start() {
        return this;
    }

    @Override
    public TestServerRouter<M, T> stop() {
        return this;
    }


}
