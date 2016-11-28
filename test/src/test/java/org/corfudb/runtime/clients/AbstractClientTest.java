package org.corfudb.runtime.clients;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.junit.Before;

/**
 * Created by mwei on 12/13/15.
 */
public abstract class AbstractClientTest extends AbstractCorfuTest {

    /**
     * Initialize the AbstractClientTest.
     */
    public AbstractClientTest() {
        // Force all new CorfuRuntimes to override the getRouterFn
       // CorfuRuntime.overrideGetRouterFunction = this::getRouterFunction;
    }

   // @Getter
   // TestClientRouter router;

  //  @Getter
   // TestServerRouter serverRouter;

    @Before
    public void resetTest() {
//        serverRouter = new TestServerRouter();
 //       router = new TestClientRouter(serverRouter);
 //       getServersForTest().stream().forEach(serverRouter::addServer);
 //       getClientsForTest().stream().forEach(router::addClient);
    }

    /**
     * A map of maps to endpoint->routers, mapped for each runtime instance captured
     */
 //   final Map<CorfuRuntime, Map<String, TestClientRouter>>
//            runtimeRouterMap = new ConcurrentHashMap<>();

    /**
     * Function for obtaining a router, given a runtime and an endpoint.
     *]
     * @return
     */
 //   private IClientRouter getRouterFunction(CorfuRuntime runtime, String endpoint) {
 //       runtimeRouterMap.putIfAbsent(runtime, new ConcurrentHashMap<>());
 //       if (!endpoint.startsWith("test:")) {
 //           throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
 //       }
 //       return runtimeRouterMap.get(runtime).computeIfAbsent(endpoint,
 //               x -> {
 //                   TestClientRouter tcn =
 //                           new TestClientRouter(serverRouter);
 //                   tcn.addClient(new BaseClient())
 //                           .addClient(new SequencerClient())
 //                           .addClient(new LayoutClient())
 //                           .addClient(new LogUnitClient())
 //                           .addClient(new ManagementClient());
 //                   return tcn;
 //               }
 //       );
 //   }

 //   abstract Set<AbstractServer> getServersForTest();

  //  abstract Set<IClient> getClientsForTest();

    public ServerContext defaultServerContext() {
        final int MAX_CACHE = 256_000_000;
        return new ServerContextBuilder()
                .setInitialToken(0)
                .setMemory(true)
                .setSingle(false)
                .setMaxCache(MAX_CACHE)
                .build();
    }
}
