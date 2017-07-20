package org.corfudb.test;


import static org.corfudb.AbstractCorfuTest.SERVERS;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 7/19/17.
 */
@RequiredArgsConstructor
public enum CorfuConfiguration {
    STANDARD("Standard",
            CorfuTestInstance.TestInstanceBuilder::standard,
            () -> new CorfuRuntime(CorfuTestInstance.getEndpoint(SERVERS.PORT_0))),
    STANDARD_CLIENT_CACHING_DISABLED("Standard - No Client Caching",
            CorfuTestInstance.TestInstanceBuilder::standard,
            () -> new CorfuRuntime(CorfuTestInstance.getEndpoint(SERVERS.PORT_0))
                    .setCacheDisabled(true)),
    STANDARD_NO_HOLE_FILL("Standard - No Hole Filling",
            CorfuTestInstance.TestInstanceBuilder::standard,
               () -> new CorfuRuntime(CorfuTestInstance.getEndpoint(SERVERS.PORT_0))
                        .setHoleFillingDisabled(true)),
    CHAIN_3("Chain (3 Replicas)",
            b -> {
                b.addServer(SERVERS.PORT_0,
                        ServerContextBuilder.defaultNonSingleContext(SERVERS.PORT_0));
                b.addServer(SERVERS.PORT_1,
                        ServerContextBuilder.defaultNonSingleContext(SERVERS.PORT_1));
                b.addServer(SERVERS.PORT_2,
                        ServerContextBuilder.defaultNonSingleContext(SERVERS.PORT_2));

                Layout layout = new TestLayoutBuilder()
                        .setEpoch(1L)
                            .addLayoutServer(SERVERS.PORT_0)
                            .addLayoutServer(SERVERS.PORT_1)
                            .addLayoutServer(SERVERS.PORT_2)
                            .addSequencer(SERVERS.PORT_0)
                        .buildSegment()
                            .buildStripe()
                            .addLogUnit(SERVERS.PORT_0)
                            .addLogUnit(SERVERS.PORT_1)
                            .addLogUnit(SERVERS.PORT_2)
                        .addToSegment()
                        .addToLayout()
                        .build();

                b.bootstrapAllServers(layout);
            },
            () -> new CorfuRuntime(CorfuTestInstance.getEndpoint(SERVERS.PORT_0))
    );

    final String descriptiveName;
    final Consumer<CorfuTestInstance.TestInstanceBuilder> serverConfiguration;
    final Supplier<CorfuRuntime> runtimeSupplier;

}
