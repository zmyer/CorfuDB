package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import org.corfudb.format.Types;

import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ServerContextBuilder;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.StreamLogFiles.METADATA_SIZE;

/**
 * Created by mwei on 12/14/15.
 */
public class LogUnitClientTest extends
        AbstractClientTest<LogUnitClient, LogUnitServer> {

    public LogUnitClientTest() {
        super(LogUnitClient::new, LogUnitServer::new);
    }

    @Test
    public void canReadWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        getClient().write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        LogData r = getClient().read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
    }

    @Test
    public void overwriteThrowsException()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        getClient().write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> getClient()
                .write(0, Collections.<UUID>emptySet(), 0,
                testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotBeOverwritten()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        getClient().fillHole(0).get();
        LogData r = getClient().read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.HOLE);

        assertThatThrownBy(() -> getClient()
                .write(0, Collections.<UUID>emptySet(),
                        0, testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotOverwrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        getClient().write(0, Collections.<UUID>emptySet(),
                0, testString, Collections.emptyMap()).get();

        LogData r = getClient()
                .read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        assertThatThrownBy(() -> getClient()
                .fillHole(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception {
        final long ADDRESS_0 = 1337L;
        final long ADDRESS_1 = 1338L;

        byte[] testString = "hello world".getBytes();
        getClient().write(0, Collections.<UUID>emptySet(), 0, testString,
                ImmutableMap.<UUID, Long>builder()
                        .put(CorfuRuntime.getStreamID("hello"), ADDRESS_0)
                        .put(CorfuRuntime.getStreamID("hello2"), ADDRESS_1)
                        .build()).get();

        LogData r = getClient()
                .read(0).get().getReadSet().get(0L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello"), ADDRESS_0);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello2"), ADDRESS_1);
    }

    @Test
    public void canCommitWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        getClient().write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        getClient().writeCommit(null, 0, true).get();
        LogData r = getClient().read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
        assertThat(r.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT));

        final long OUTSIDE_ADDRESS = 10L;
        UUID streamA = CorfuRuntime.getStreamID("streamA");
        getClient().writeStream(1, Collections.singletonMap(streamA, 0L), testString).get();
        getClient().writeCommit(Collections.singletonMap(streamA, 0L), OUTSIDE_ADDRESS, true).get(); // 10L shouldn't matter

        r = getClient().read(streamA, Range.singleton(0L)).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
        assertThat(r.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT));
    }

    @Test
    public void CorruptedDataReadThrowsException() throws Exception {
        // Force the log unit to start with persistence.
        resetServer(new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .build());

        byte[] testString = "hello world".getBytes();
        getClient().write(0, Collections.<UUID>emptySet(), 0,
                testString, Collections.emptyMap()).get();
        LogData r = getClient().read(0).get().getReadSet().get(0L);
        // Verify that the data has been written correctly
        assertThat(r.getPayload(null)).isEqualTo(testString);

        // In order to clear the logunit's cache, the server is restarted so that
        // the next read is forced to be retrieved from file and not the cache
        resetServer(new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .build());

        // Corrupt the written log entry
        String logFilePath = PARAMETERS.TEST_TEMP_DIR + File.separator +
                                                                "log/0.log";
        RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");

        ByteBuffer metaDataBuf = ByteBuffer.allocate(METADATA_SIZE);
        file.getChannel().read(metaDataBuf);
        metaDataBuf.flip();

        Types.Metadata metadata = Types.Metadata.parseFrom(metaDataBuf.array());
        final int fileOffset = Integer.BYTES + METADATA_SIZE + metadata.getLength() + 20;
        final int CORRUPT_BYTES = 0xFFFF;
        file.seek(fileOffset); // File header + delimiter
        file.writeInt(CORRUPT_BYTES);
        file.close();

        // Try to read a corrupted log entry
        assertThatThrownBy(() -> getClient().read(0).get())
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(DataCorruptionException.class);
    }
}
