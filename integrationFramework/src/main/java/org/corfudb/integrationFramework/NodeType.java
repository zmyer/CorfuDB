package org.corfudb.integrationFramework;

/**
 * Created by zlokhandwala on 6/5/17.
 */
public enum NodeType {

    // Allocates node locally as a process.
    PHYSICAL,
    // Allocates node on a VM.
    VM,
    // Allocates node on a container.
    CONTAINER;
}
