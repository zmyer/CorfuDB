package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;

import org.assertj.core.data.MapEntry;
import org.corfudb.test.InterfaceTest;
import org.corfudb.test.Iterations;

/**
 * Created by mwei on 7/18/17.
 */
public interface MapContract<M extends Map<K, V>, K, V> extends TestableMap<M, K, V> {

    @InterfaceTest
    default void emptyHasSize0(M map) {
        assertThat(map)
                .hasSize(0);
    }

    @InterfaceTest
    default void emptyIsEmpty(M map) {
        assertThat(map)
                .isEmpty();
    }

    @InterfaceTest
    default void canClearEmptyMap(M map) {
        map.clear();
        assertThat(map)
                .isEmpty();
        assertThat(map)
                .hasSize(0);
    }

    @InterfaceTest
    default void canRemove(M map, K k1, V v1) {
        map.put(k1, v1);
        map.remove(k1);

        assertThat(map.get(k1))
                .isNull();
        assertThat(map.containsKey(k1))
                .isFalse();
        assertThat(map)
                .isEmpty();
        assertThat(map)
                .hasSize(0);
    }

    @InterfaceTest
    default void canPut(M map, K k1, V v1) {
        map.put(k1, v1);

        assertThat(map)
                .containsExactly(MapEntry.entry(k1, v1));
    }

    @InterfaceTest
    default void canClearPut(M map, K k1, V v1) {
        map.put(k1, v1);
        map.clear();

        assertThat(map)
                .isEmpty();
        assertThat(map)
                .hasSize(0);
    }

    @InterfaceTest
    default void canPutMultiple(M map, @Iterations int totalIterations) {
        for (int i = 0; i < totalIterations; i++) {
            map.put(createKey(), createValue());
        }
        assertThat(map)
                .hasSize(totalIterations);
    }

    @InterfaceTest
    default void putHasPreviousValue(M map, K k1, V v1, V v2) {
        assertThat(map.put(k1, v1))
                .isNull();
        assertThat(map.put(k1, v2))
                .isEqualTo(v1);
    }

    @InterfaceTest
    default void removeHasPreviousValue(M map, K k1, V v1) {
        assertThat(map.put(k1, v1))
                .isNull();
        assertThat(map.remove(k1))
                .isEqualTo(v1);
    }

    @InterfaceTest
    default void canPutAllEmptyMap(M map) {
        map.putAll(Collections.emptyMap());
    }

}
