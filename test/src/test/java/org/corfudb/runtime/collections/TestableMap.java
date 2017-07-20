package org.corfudb.runtime.collections;

import java.util.Map;

import org.corfudb.test.TestableInterface;

/**
 * Created by mwei on 7/18/17.
 */
@TestableInterface
public interface TestableMap<M extends Map<K,V>, K, V> {
    M createMap();
    K createKey();
    V createValue();
}
