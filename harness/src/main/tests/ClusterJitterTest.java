package org.corfudb.harness.tests;

import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;
import src.main.orchestration.Cluster;

/**
 * Created by box on 8/25/17.
 */
public class ClusterJitterTest {

    @Test
    public void test() throws Exception {
        Cluster cluster = new Cluster("localhost", 9000);
        cluster.createSingleNode();

        //CorfuRuntime corfuRuntime = cluster.getRuntime();
        cluster.injectJitter();
    }
}
