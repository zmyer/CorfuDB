package org.corfudb.integrationFramework;

import org.corfudb.runtime.view.Layout;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zlokhandwala on 6/8/17.
 */
public class DeploymentEngine {

    NodeFactory nodeFactory;
    Driver driver;

    /**
     * Sets up the cluster using the preset driver.
     *
     * @param layout
     * @return Completable future completing upon cluster setup.
     */
    public CompletableFuture<Boolean> setupCluster(Layout layout) {
        return null;
    }

    /**
     * Sets up the cluster using the preset driver
     * with the provided node configurations.
     *
     * @param layout
     * @param nodeConfigs
     * @return Completable future completing upon cluster setup.
     */
    public CompletableFuture<Boolean> setupCluster(Layout layout, Collection<NodeConfig> nodeConfigs) {
        return null;
    }

    /**
     * Tears down and frees resources held by the cluster.
     *
     * @param clusterId
     * @return Completable future completing upon cluster tear-down.
     */
    public CompletableFuture<Boolean> tearDownCluster(UUID clusterId) {
        return null;
    }
}
