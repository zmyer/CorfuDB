package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;

import java.util.concurrent.atomic.AtomicInteger;

import org.corfudb.test.CorfuTestClass;
import org.corfudb.test.ICorfuTest;

/**
 * Created by mwei on 7/19/17.
 */
@CorfuTestClass
public class SmrMapContractTest implements
        MapContract<SMRMap<Integer, Integer>, Integer, Integer>, ICorfuTest {

    AtomicInteger mapCounter = new AtomicInteger();
    @Override
    public SMRMap<Integer, Integer> createMap() {
        return getUtil().getNewRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                .setStreamName("test" + mapCounter.getAndIncrement())
                .open();
    }

    AtomicInteger keyCounter = new AtomicInteger();
    @Override
    public Integer createKey() {
        return keyCounter.getAndIncrement();
    }

    AtomicInteger valueCounter = new AtomicInteger();
    @Override
    public Integer createValue() {
        return valueCounter.getAndIncrement();
    }
}
