package org.corfudb.runtime.checkpoint;

import lombok.extern.slf4j.Slf4j;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.assertj.core.api.ThrowableAssert;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by dmalkhi on 5/25/17.
 */
@Slf4j
public class CheckpointTest extends AbstractCheckpointTest {

    /**
     * This test verifies that a client that recovers a map from checkpoint,
     * but wants the map at a snapshot -earlier- than the snapshot,
     * will either get a transactionAbortException, or get the right version of
     * the map.
     * <p>
     * It works as follows.
     * We build a map with one hundred entries [0, 1, 2, 3, ...., 99].
     * <p>
     * Note that, each entry is one put, so if a TX starts at snapshot at 77, it should see a map with 77 items 0, 1, 2, ..., 76.
     * We are going to verify that this works even if we checkpoint the map, and trim a prefix, say of the first 50 put's.
     * <p>
     * First, we then take a checkpoint of the map.
     * <p>
     * Then, we prefix-trim the log up to position 50.
     * <p>
     * Now, we start a new runtime and instantiate this map. It should build the map from a snapshot.
     * <p>
     * Finally, we start a snapshot-TX at timestamp 77. We verify that the map state is [0, 1, 2, 3, ..., 76].
     */
    @Test
    public void undoCkpointTest() throws Exception {

        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int trimPosition = mapSize / 2;
        final int snapshotPosition = trimPosition + 2;

        t(1, () -> {
            CorfuRuntime myRuntime = getMyRuntime();
            Map<String, Long> m2A = instantiateMap(myRuntime, "m2A");

            // first, populate the map
            for (int i = 0; i < mapSize; i++) {
                m2A.put(String.valueOf(i), (long) i);
            }

            // now, take a checkpoint and perform a prefix-trim
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap((SMRMap) m2A);
            long checkpointAddress = mcw1.appendCheckpoints(getRuntime(), "dahlia");

            // Trim the log
            myRuntime.getAddressSpaceView().prefixTrim(trimPosition);
            myRuntime.getAddressSpaceView().gc();
            myRuntime.getAddressSpaceView().invalidateServerCaches();
            myRuntime.getAddressSpaceView().invalidateClientCache();
        }
        );

        AtomicBoolean trimExceptionFlag = new AtomicBoolean(false);

        // start a new runtime
        t(2, () -> {
            CorfuRuntime myRuntime2 = setNewRuntime();
            Map<String, Long> localm2A = instantiateMap(myRuntime, "m2A");

            // start a snapshot TX at position snapshotPosition
            myRuntime2.getObjectsView().TXBuild()
                    .setType(TransactionType.SNAPSHOT)
                    .setSnapshot(snapshotPosition - 1)
                    .begin();

            // finally, instantiate the map for the snapshot and assert is has the right state
            try {
                localm2A.get(0);
            } catch (TransactionAbortedException te) {
                // this is an expected behavior!
                trimExceptionFlag.set(true);
            }

            if (trimExceptionFlag.get() == false) {
                assertThat(localm2A.size())
                        .isEqualTo(snapshotPosition);

                // check map positions 0..(snapshot-1)
                for (int i = 0; i < snapshotPosition; i++) {
                    assertThat(localm2A.get(String.valueOf(i)))
                            .isEqualTo((long) i);
                }

                // check map positions snapshot..(mapSize-1)
                for (int i = snapshotPosition; i < mapSize; i++) {
                    assertThat(localm2A.get(String.valueOf(i)))
                            .isEqualTo(null);
                }
            }
        }
        );

    }

    /**
     * This test intentionally "delays" a checkpoint, to allow additional
     * updates to be appended to the stream after.
     * <p>
     * It works as follows. First, a transcation is started in order to set a
     * snapshot time.
     * <p>
     * Then, some updates are appended.
     * <p>
     * Finally, we take checkpoints. Since checkpoints occur within
     * transactions, they will be nested inside the outermost transaction.
     * Therefore, they will inherit their snapshot time from the outermost TX.
     *
     * @throws Exception
     */
    @Test
    public void delayedCkpointTest() throws Exception {
        CorfuRuntime myRuntime = getMyRuntime();

        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int additional = mapSize / 2;

        SMRMap<String, Long> m2A = instantiateMap(myRuntime,"m2A");

        // first, populate the map
        for (int i = 0; i < mapSize; i++) {
            m2A.put(String.valueOf(i), (long) i);
        }

        // in one thread, start a snapshot transaction and leave it open
        t(1, () -> {
            // start a snapshot TX at position snapshotPosition
            myRuntime.getObjectsView().TXBuild()
                    .setType(TransactionType.SNAPSHOT)
                    .setSnapshot(mapSize - 1)
                    .begin();
                }
        );

        // now delay
        // in another thread, introduce new updates to the map
        t(2, () -> {
            for (int i = 0; i < additional; i++) {
                        m2A.put(String.valueOf(mapSize+i), (long) (mapSize+i));
                    }
                }
        );

        // back in the first thread, checkpoint and trim
        t(1, () -> {
            // now, take a checkpoint and perform a prefix-trim
            long checkpointAddress = mapCkpoint(myRuntime, m2A);
            // Trim the log
            logTrim(myRuntime, checkpointAddress);

            myRuntime.getObjectsView().TXEnd();

        });

        // finally, verify that a thread can build the map correctly
        t(2, () -> {
            CorfuRuntime myRuntime2 = setNewRuntime();

            SMRMap<String, Long> localm2A = instantiateMap(myRuntime2, "m2A");

            assertThat(localm2A.size())
                    .isEqualTo(mapSize+additional);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)))
                        .isEqualTo((long) i);
            }
            for (int i = mapSize; i < mapSize+additional; i++) {
                assertThat(localm2A.get(String.valueOf(i)))
                        .isEqualTo((long) i);
            }

        });
    }

    @Test
    public void prefixTrimTwiceAtSameAddress() throws Exception {
        CorfuRuntime myRuntime = getMyRuntime();

        final int mapSize = 5;

        SMRMap<String, Long> m2A = instantiateMap(myRuntime, "m2A");

        // first, populate the map
        for (int i = 0; i < mapSize; i++) {
            m2A.put(String.valueOf(i), (long) i);
        }

        // Trim again in exactly the same place shouldn't fail
        myRuntime.getAddressSpaceView().prefixTrim(2);
        myRuntime.getAddressSpaceView().prefixTrim(2);

        // GC twice at the same place should be fine.
        myRuntime.getAddressSpaceView().gc();
        myRuntime.getAddressSpaceView().gc();
    }

    @Test
    public void transactionalReadAfterCheckpoint() throws Exception {
        CorfuRuntime myRuntime = getMyRuntime();

        SMRMap<String, String> testMap = myRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .setStreamName("test")
                .open();

        SMRMap<String, String> testMap2 = myRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .setStreamName("test2")
                .open();

        // Place entries into the map
        testMap.put("a", "a");
        testMap.put("b", "a");
        testMap.put("c", "a");
        testMap2.put("a", "c");
        testMap2.put("b", "c");
        testMap2.put("c", "c");

        // Insert a checkpoint
        long checkpointAddress = mapCkpoint(myRuntime, testMap, testMap2);

        // TX1: Move object to 1
        myRuntime.getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(1)
                .begin();

        testMap.get("a");
        myRuntime.getObjectsView().TXEnd();

        // Trim the log
        logTrim(myRuntime, checkpointAddress - 1);

        // TX2: Read most recent state in TX
        myRuntime.getObjectsView().TXBegin();
        testMap.put("a", "b");
        myRuntime.getObjectsView().TXEnd();
    }
}
