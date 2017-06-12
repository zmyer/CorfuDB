package org.corfudb.integrationFramework;

import org.junit.Test;

/**
 * Created by zlokhandwala on 6/9/17.
 */
public class NodeConfigTest {

    @Test
    public void nodeConfigBuilderExample() throws Exception {
        NodeConfig n = NodeConfig.builder()
                .port(9000L)
                .build();
        System.out.println(n.getOptions());
    }
}
