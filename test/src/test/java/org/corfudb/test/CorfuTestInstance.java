package org.corfudb.test;

import static org.corfudb.AbstractCorfuTest.SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;

/**
 * Created by mwei on 7/14/17.
 */
public class CorfuTestInstance {

    final ExtensionContext context;

    /** A map of parameter indexes to generated runtimes
     *  which are available.
     */
    final Map<Integer, CorfuRuntime> generatedRuntimes = new HashMap<>();

    /** A map of the current test servers, by endpoint name */
    final Map<String, CorfuTestInstance.TestServer> testServerMap = new ConcurrentHashMap<>();

    /** A map of maps to endpoint->routers, mapped for each runtime instance captured */
    final Map<CorfuRuntime, Map<String, NettyClientRouter>>
            runtimeRouterMap = new ConcurrentHashMap<>();

    final CorfuConfiguration configuration;

    public CorfuTestInstance(ExtensionContext context, CorfuConfiguration configuration) {
        this.context = context;
        this.configuration = configuration;
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
        configuration.serverConfiguration.accept(new TestInstanceBuilder());
    }

    public CorfuRuntime getNewRuntime() {
        CorfuRuntime rt = configuration.runtimeSupplier.get().connect();
        int i = 0;
        while (generatedRuntimes.containsKey(i)) {
            i++;
        }
        generatedRuntimes.put(i, rt);
        return rt;
    }

    public CorfuRuntime getRuntimeAsParameter(ParameterContext pContext) {
        if (generatedRuntimes.get(pContext.getIndex()) != null) {
            return generatedRuntimes.get(pContext.getIndex());
        }
        CorfuRuntime rt = configuration.runtimeSupplier.get().connect();
        generatedRuntimes.put(pContext.getIndex(),rt);
        return rt;
    }

    /** Get the endpoint string, given a port number.
     *
     * @param port  The port number to get an endpoint string for.
     * @return      The endpoint string.
     */
    public static String getEndpoint(int port) {
        return "test:" + port;
    }

    /** Function for obtaining a router, given a runtime and an endpoint.
     *
     * @param runtime       The CorfuRuntime to obtain a router for.
     * @param endpoint      An endpoint string for the router.
     * @return
     */
    private IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
        runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
        if (!endpoint.startsWith("test:")) {
            throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
        }
        return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint,
                x -> {
                    NettyClientRouter r = spy(new NettyClientRouter(endpoint));
                    r.channel = mock(Channel.class);
                    r.context = mock(ChannelHandlerContext.class);
                    r.connected = true;
                    when(r.channel.writeAndFlush(any()))
                            .then(a -> {
                                ((TestServerRouter)testServerMap.get(endpoint).serverRouter)
                                                .sendServerMessage(a.getArgument(0), r.context);
                                        return null;
                                    }
                            );
                    when(r.context.writeAndFlush(any()))
                            .then(a -> {
                                r.channelRead(r.context, a.getArgument(0));
                                return null;
                            });

                    r.addClient(new BaseClient())
                            .addClient(new SequencerClient())
                            .addClient(new LayoutClient())
                            .addClient(new LogUnitClient())
                            .addClient(new ManagementClient());
                    return r;
                }
        );
    }

    public class TestInstanceBuilder {

        void standard() {
            testServerMap.put(getEndpoint(SERVERS.PORT_0), new TestServer(SERVERS.PORT_0));
        }

        void addServer(int port) {
            testServerMap.put(getEndpoint(port), new TestServer(port));
        }

        void addServer(int port, ServerContext context) {
            testServerMap.put(getEndpoint(port), new TestServer(context.getServerConfig()));
        }

        void bootstrapAllServers(Layout l) {
            testServerMap.entrySet().parallelStream()
                    .forEach(e -> {
                        e.getValue().layoutServer
                                .handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l)),
                                        null, e.getValue().serverRouter);
                        e.getValue().managementServer
                                .handleMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(l),
                                        null, e.getValue().serverRouter);
                    });
        }
    }

    /**
     * This class holds instances of servers used for test.
     */
    @Data
    private static class TestServer {
        ServerContext serverContext;
        BaseServer baseServer;
        SequencerServer sequencerServer;
        LayoutServer layoutServer;
        LogUnitServer logUnitServer;
        ManagementServer managementServer;
        IServerRouter serverRouter;
        int port;

        TestServer(Map<String, Object> optsMap)
        {
            this(new ServerContext(optsMap, new TestServerRouter()));
        }

        TestServer(ServerContext serverContext) {
            this.serverContext = serverContext;
            this.serverRouter = serverContext.getServerRouter();
            this.baseServer = new BaseServer();
            this.sequencerServer = new SequencerServer(serverContext);
            this.layoutServer = new LayoutServer(serverContext);
            this.logUnitServer = new LogUnitServer(serverContext);
            this.managementServer = new ManagementServer(serverContext);

            this.serverRouter.addServer(baseServer);
            this.serverRouter.addServer(sequencerServer);
            this.serverRouter.addServer(layoutServer);
            this.serverRouter.addServer(logUnitServer);
            this.serverRouter.addServer(managementServer);
        }

        TestServer(int port)
        {
            this(ServerContextBuilder.defaultContext(port).getServerConfig());
        }

        public TestServerRouter getServerRouter() {
            return (TestServerRouter) this.serverRouter;
        }
    }
}
