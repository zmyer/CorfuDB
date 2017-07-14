package org.corfudb.test;

import static org.corfudb.AbstractCorfuTest.SERVERS;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;

import lombok.Data;

/**
 * Created by mwei on 7/14/17.
 */
public class CorfuTestInstance {

    final ExtensionContext context;

    /** A map of the current test servers, by endpoint name */
    final Map<String, CorfuTestInstance.TestServer> testServerMap = new ConcurrentHashMap<>();

    /** A map of maps to endpoint->routers, mapped for each runtime instance captured */
    final Map<CorfuRuntime, Map<String, TestClientRouter>>
            runtimeRouterMap = new ConcurrentHashMap<>();

    public CorfuTestInstance(ExtensionContext context) {
        this.context = context;
        CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
        testServerMap.put(getEndpoint(SERVERS.PORT_0), new TestServer(SERVERS.PORT_0));
    }

    public CorfuRuntime getRuntimeAsParameter(ParameterContext pContext) {
        return new CorfuRuntime(getEndpoint(SERVERS.PORT_0)).connect();
    }

    /** Get the endpoint string, given a port number.
     *
     * @param port  The port number to get an endpoint string for.
     * @return      The endpoint string.
     */
    public String getEndpoint(int port) {
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
                    TestClientRouter tcn =
                            new TestClientRouter(testServerMap.get(endpoint).getServerRouter());
                    tcn.addClient(new BaseClient())
                            .addClient(new SequencerClient())
                            .addClient(new LayoutClient())
                            .addClient(new LogUnitClient())
                            .addClient(new ManagementClient());
                    return tcn;
                }
        );
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
