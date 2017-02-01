package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.format.Types.NodeMetrics;
import org.corfudb.infrastructure.*;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the Management client.
 * <p>
 * Created by zlokhandwala on 11/7/16.
 */
public class ManagementClientTest extends AbstractClientTest<ManagementClient, ManagementServer> {

    public ManagementClientTest() {
        super(ManagementClient::new, ManagementServer::new);
    }

    @Override
    public ServerContext getServerContext() {
        final int MAX_CACHE = 256_000_000;
        return new ServerContextBuilder()
                .setInitialToken(0)
                .setMemory(true)
                .setSingle(true)
                .setMaxCache(MAX_CACHE)
                .build();
    }

    /**
     * Need to shutdown the servers after test.
     */
    @After
    public void cleanUp() {
        getServer().shutdown();
    }

    /**
     * Tests the bootstrapping of the management server.
     *
     * @throws Exception
     */
    @Test
    public void handleBootstrap()
            throws Exception {
        // Since the servers are started as single nodes thus already bootstrapped.
        assertThatThrownBy(() -> getClient().bootstrapManagement(TestLayoutBuilder.single(SERVERS.PORT_0)).get()).isInstanceOf(ExecutionException.class);
    }

    /**
     * Tests the msg handler for failure detection.
     *
     * @throws Exception
     */
    @Test
    public void handleFailure()
            throws Exception {

        // Since the servers are started as single nodes thus already bootstrapped.
        Map map = new HashMap<String, Boolean>();
        map.put("Key", true);
        assertThat(getClient().handleFailure(map).get()).isEqualTo(true);
    }

    /**
     * Tests the failure handler start trigger.
     *
     * @throws Exception
     */
    @Test
    public void initiateFailureHandler()
            throws Exception {
        assertThat(getClient().initiateFailureHandler().get()).isEqualTo(true);
    }

    /**
     * Tests the heartbeat request and asserts if response is received.
     *
     * @throws Exception
     */
    @Test
    public void sendHeartbeatRequest()
            throws Exception {
        byte[] buffer = getClient().sendHeartbeatRequest().get();
        assertThat(NodeMetrics.parseFrom(buffer)).isNotNull();
    }
}
