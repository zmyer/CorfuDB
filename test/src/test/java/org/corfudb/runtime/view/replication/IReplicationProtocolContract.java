package org.corfudb.runtime.view.replication;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.CorfuTestClass;

/**
 * Created by mwei on 7/17/17.
 */
@CorfuTestClass
public interface IReplicationProtocolContract extends IReplicationProtocol {

    @CorfuTest
    default void readReturnsHoleFill(CorfuRuntime rt) {
    }
}
