package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by mwei on 2/4/16.
 */
public class LogUnitServerTest extends AbstractServerTest<LogUnitServer> {

    public LogUnitServerTest() {
        super(LogUnitServer::new);
    }


    @Test
    public void checkHeapLeak() throws Exception {
        long address = 0L;
        final byte TEST_BYTE = 42;
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer(1);
        b.writeByte(TEST_BYTE);
        WriteRequest wr = WriteRequest.builder()
                            .writeMode(WriteMode.NORMAL)
                            .data(new LogData(DataType.DATA, b))
                            .build();
        //write at 0
        wr.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        wr.setRank(0L);
        wr.setBackpointerMap(Collections.emptyMap());
        wr.setGlobalAddress(0L);

        sendMessage(CorfuMsgType.WRITE.payloadMsg(wr));

      //  LoadingCache<LogAddress, LogData> dataCache = s1.getDataCache();
        // Make sure that extra bytes are truncated from the payload byte buf
    //    Assertions.assertThat(dataCache.get(new LogAddress(address, null)).getData().capacity()).isEqualTo(1);
    }

}

