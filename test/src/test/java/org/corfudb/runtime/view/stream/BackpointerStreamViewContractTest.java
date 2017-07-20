package org.corfudb.runtime.view.stream;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.ICorfuTest;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ICorfuSerializable;

import io.netty.buffer.ByteBuf;
import lombok.Data;

public class BackpointerStreamViewContractTest implements
        StreamViewContract<BackpointerStreamView, BackpointerStreamViewContractTest.LongWrapper>,
        ICorfuTest {

    @Data
    static class LongWrapper implements ICorfuSerializable {
        final long value;

        @Override
        public void serialize(ByteBuf b) {
            b.writeBytes(Utils.longToBigEndianByteArray(value));
        }

        public static ICorfuSerializable deserialize(ByteBuf b, CorfuRuntime rt) {
            return new LongWrapper(b.readLong());
        }
    }

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
    public LongWrapper createEntry() {
        return new LongWrapper(entryCounter.getAndIncrement());
    }
}
