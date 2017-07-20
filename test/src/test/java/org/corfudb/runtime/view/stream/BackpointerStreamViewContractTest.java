package org.corfudb.runtime.view.stream;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.ICorfuTest;
import org.corfudb.util.Utils;

public class BackpointerStreamViewContractTest implements
        StreamViewContract<BackpointerStreamView, byte[]>,
        ICorfuTest {


    final AtomicInteger streamNumber = new AtomicInteger(0);

    @Override
    public BackpointerStreamView createStreamView() {
        return createStreamView(streamNumber.getAndIncrement(), false);
    }

    @Override
    public BackpointerStreamView createStreamView(int index, boolean existing) {
        if (existing) {
            throw new UnsupportedOperationException("Cannot open existing stream yet");
        }
        return (BackpointerStreamView) getUtil().getNewRuntime()
                .getStreamsView().get(
                        CorfuRuntime.getStreamID("test-" + index));
    }

    final AtomicLong entryCounter = new AtomicLong(0L);
    @Override
    public byte[] createEntry() {
        return Utils.longToBigEndianByteArray(entryCounter.getAndIncrement());
    }
}
