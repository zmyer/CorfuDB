package org.corfudb.harness;

import org.corfudb.harness.gen.FaultType;
import org.corfudb.harness.gen.Instance;
import org.corfudb.runtime.CorfuRuntime;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by box on 8/25/17.
 */
public class Cluster {

    private final Harness harness;
    private final List<Instance> nodes;

    public Cluster(String address, int port) throws Exception {
        harness = new Harness(address, port);
        nodes = new ArrayList();
    }

    public void createSingleNode() throws Exception {
        Instance instance = harness.create("");
        nodes.add(instance);
    }

    public CorfuRuntime getRuntime() {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("Cluster has no nodes!");
        }

        String connectStr = "";
        for (Instance instance : nodes) {
            connectStr += instance.getAddress() + instance.getPort() + ",";
        }

        return new CorfuRuntime(connectStr.substring(0, connectStr.length() - 1));
    }

    public void injectJitter() throws Exception {
        for (Instance instance : nodes) {
            harness.injectFault(FaultType.Jitter, instance);
        }
    }

    public void destroy() throws Exception {
        for (Instance instance : nodes) {
            harness.remove(instance);
        }
    }

}
