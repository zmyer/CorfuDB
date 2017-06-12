package org.corfudb.integrationFramework;

import java.util.concurrent.CompletableFuture;

/**
 * Created by zlokhandwala on 6/8/17.
 */
public interface Driver {

    /**
     * Creates and allocates resources for the nodeConfig.
     * Setting up a VM, allocating resources for containers, etc.
     *
     * @param nodeConfig
     * @return Completable future completing upon node creation.
     */
    CompletableFuture<Boolean> createNode(NodeConfig nodeConfig);

    /**
     * Starts the nodeConfig.
     *
     * @param nodeConfig
     * @return Completable future completing upon node startup.
     */
    CompletableFuture<Boolean> startNode(NodeConfig nodeConfig);

    /**
     * Suspends or pauses nodeConfig functionality.
     *
     * @param nodeConfig
     * @return Completable future completing upon node suspension.
     */
    CompletableFuture<Boolean> suspendNode(NodeConfig nodeConfig);

    /**
     * Stops the nodeConfig but the resources are not freed.
     *
     * @param nodeConfig
     * @return Completable future completing upon node stop.
     */
    CompletableFuture<Boolean> stopNode(NodeConfig nodeConfig);

    /**
     * De-allocates the nodeConfig resources.
     *
     * @param nodeConfig
     * @return Completable future completing upon node destruction.
     */
    CompletableFuture<Boolean> destroyNode(NodeConfig nodeConfig);

    /**
     * Runs the command on the nodeConfig.
     * Can be used to inject faults, monitor state,
     * fetch results, etc.
     *
     * @param nodeConfig
     * @return Completable future completing upon command completion.
     */
    CompletableFuture<Boolean> runCommand(NodeConfig nodeConfig);
}
