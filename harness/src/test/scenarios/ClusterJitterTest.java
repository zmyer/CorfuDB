package scenarios;

import org.corfudb.harness.Cluster;

/**
 * Created by box on 8/25/17.
 */
public class ClusterJitterTest {

    public void test() throws Exception {
        Cluster cluster = new Cluster("localhost", 9000);
        cluster.createSingleNode();

        //CorfuRuntime corfuRuntime = cluster.getRuntime();
        cluster.injectJitter();
    }
}
