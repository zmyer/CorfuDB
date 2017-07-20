package org.corfudb.test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.corfudb.runtime.CorfuRuntime;

import lombok.Data;

/**
 * Created by mwei on 7/19/17.
 */
public interface ICorfuTest {

    @Data
    class CorfuTestUtil {
        private final Supplier<CorfuRuntime> runtimeSupplier;

        public CorfuRuntime getNewRuntime() {
            return runtimeSupplier.get();
        }
    }

    AtomicReference<CorfuTestUtil> testUtil = new AtomicReference<>();

    default CorfuTestUtil getUtil() {
        return testUtil.get();
    }

    default void setUtil(CorfuTestUtil util) {
        testUtil.set(util);
    }
}
