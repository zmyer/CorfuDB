package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/18/16.
 */
public class LogUnitPersistenceTest extends AbstractServerTest<LogUnitServer> {

    /**
     * Get a persistent server context.
     *
     * @return A new server context.
     */
    @Override
    public ServerContext getServerContext() {
        return new ServerContextBuilder()
                .setCheckpoint(1)
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .build();
    }


    public LogUnitPersistenceTest() {super(LogUnitServer::new);}


    @Test
    public void checkThatWritesArePersisted()
            throws Exception {

        final long LOW_ADDRESS = 0L;
        final long MID_ADDRESS = 100L;
        final long HIGH_ADDRESS = 10000000L;

        //write at 0
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(LOW_ADDRESS);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        //100
        b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("100".getBytes(), b);
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(MID_ADDRESS);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        //and 10000000
        b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("10000000".getBytes(), b);
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(HIGH_ADDRESS);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        // Reset and get a new server.
        resetTest();

        assertThat(getServer())
            .containsDataAtAddress(0)
            .containsDataAtAddress(100)
            .containsDataAtAddress(10000000);

        assertThat(getServer())
            .matchesDataAtAddress(0, "0".getBytes())
            .matchesDataAtAddress(100, "100".getBytes())
            .matchesDataAtAddress(10000000, "10000000".getBytes());
    }

    @Test
    public void checkOverwritesFail() throws Exception {

        final long ADDRESS_0 = 0L;
        final long ADDRESS_1 = 100L;
        //write at 0
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        // m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setStreams(Collections.EMPTY_SET);
        m.setRank(ADDRESS_0);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        assertThat(getServer())
                .containsDataAtAddress(ADDRESS_0);
        assertThat(getServer())
                .isEmptyAtAddress(ADDRESS_1);


        // repeat: this should throw an exception
        WriteRequest m2 = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m2.setGlobalAddress(ADDRESS_0);
        // m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m2.setStreams(Collections.EMPTY_SET);
        m2.setRank(ADDRESS_0);
        m2.setBackpointerMap(Collections.emptyMap());

        sendMessage(CorfuMsgType.WRITE.payloadMsg(m2));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.OVERWRITE_ERROR);
    }



    private String createLogFile(String path, int version, boolean noVerify) throws IOException {
        // Generate a log file and manually change the version
        File logDir = new File(path + File.separator + "log");
        logDir.mkdir();

        // Create a log file with an invalid log version
        String logFilePath = logDir.getAbsolutePath() + File.separator + 0 + ".log";
        File logFile = new File(logFilePath);
        logFile.createNewFile();
        RandomAccessFile file = new RandomAccessFile(logFile, "rw");
        StreamLogFiles.writeHeader(file.getChannel(), version, noVerify);
        file.close();

        return logFile.getAbsolutePath();
    }

    @Test (expected = RuntimeException.class)
    public void testInvalidLogVersion() throws Exception {
        // Create a log file with an invalid version
        String tempDir = PARAMETERS.TEST_TEMP_DIR;
        createLogFile(tempDir, StreamLogFiles.VERSION + 1, false);

        // Start a new logging version
        ServerContextBuilder builder = new ServerContextBuilder();
        builder.setMemory(false);
        builder.setLogPath(tempDir);
        ServerContext context = builder.build();
        // LogUnitServer logunit = new LogUnitServer(context);
    }

    @Test (expected = RuntimeException.class)
    public void testVerifyWithNoVerifyLog() throws Exception {
        boolean noVerify = true;

        // Generate a log file without computing the checksum for log entries
        String tempDir = PARAMETERS.TEST_TEMP_DIR;
        createLogFile(tempDir, StreamLogFiles.VERSION + 1, noVerify);

        // Start a new logging version
        ServerContextBuilder builder = new ServerContextBuilder();
        builder.setMemory(false);
        builder.setLogPath(tempDir);
        builder.setNoVerify(!noVerify);
        ServerContext context = builder.build();
        //   LogUnitServer logunit = new LogUnitServer(context);
    }
}
