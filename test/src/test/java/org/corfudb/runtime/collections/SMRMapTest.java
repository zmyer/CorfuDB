package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.data.MapEntry;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.CorfuTestClass;
import org.corfudb.test.LoggingLevel;
import org.junit.jupiter.api.DisplayName;

/**
 * Created by mwei on 7/17/17.
 */
@CorfuTestClass
@DisplayName("SMR Map Test")
public class SMRMapTest {

    @CorfuTest
    @DisplayName("Can put item in map")
    public void canPut(SMRMap<String,String> map) {
        map.put("k1", "v1");

        assertThat(map)
                .containsExactly(MapEntry.entry("k1", "v1"));
    }

    @LoggingLevel(level= LoggingLevel.Level.WARN)
    @CorfuTest
    @DisplayName("Can put multiple items in map")
    public void canPutMultiple(SMRMap map) {
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        map.put("k4", "v4");
    }
}
