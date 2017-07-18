package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.checkpoint.AbstractCheckpointTest;
import org.corfudb.runtime.checkpoint.CPSerializer;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 7/16/17.
 */
public class ContinuousIT extends AbstractCheckpointTest {
    final String streamNameA = "mystreamA";
    final String streamNameB = "mystreamB";
    final String author = "ckpointTest";
    protected SMRMap<String, Long> m2A;
    protected SMRMap<String, Long> m2B;

    /**
     * common initialization for tests: establish Corfu runtime and instantiate two maps
     */
    @Before
    public void instantiateMaps() {
        m2A = instantiateMap(getMyRuntime(), streamNameA);
        m2B = instantiateMap(getMyRuntime(), streamNameB);
    }

    /**
     *  Start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     * @param mapSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int mapSize, boolean expectedFullsize) {
        CorfuRuntime currentRuntime = setNewRuntime();
        try {
            Map<String, Long> localm2A = instantiateMap(currentRuntime, streamNameA);
            Map<String, Long> localm2B = instantiateMap(currentRuntime, streamNameB);
            for (int i = 0; i < localm2A.size(); i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
            }
            for (int i = 0; i < localm2B.size(); i++) {
                assertThat(localm2B.get(String.valueOf(i))).isEqualTo(0L);
            }
            if (expectedFullsize) {
                assertThat(localm2A.size()).isEqualTo(mapSize);
                assertThat(localm2B.size()).isEqualTo(mapSize);
            }
        } catch (TrimmedException te) {
            // shouldn't happen
            te.printStackTrace();
            throw te;
        }
    }

    /**
     * initialize the two maps, the second one is all zeros
     * @param mapSize
     */
    void populateMaps(int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            try {
                m2A.put(String.valueOf(i), (long) i);
                m2B.put(String.valueOf(i), (long) 0);
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }
        }
    }

    /**
     * this test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 3. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    @Test
    public void periodicCkpointTest() throws Exception {
        CorfuRuntime currentRuntime = getMyRuntime();
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(mapSize);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                mapCkpoint(currentRuntime, m2A, m2B);
            }
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true);
    }

    /**
     * this test builds two maps, m2A m2B, and brings up two threads:
     * <p>
     * 1. one thread performs ITERATIONS_VERY_LOW checkpoints
     * 2. one thread repeats ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
     * they should be empty.
     * <p>
     * Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
     * Then verify they are empty.
     *
     * @throws Exception
     */
    @Test
    public void emptyCkpointTest() throws Exception {
        final int mapSize = 0;

        scheduleConcurrently(1, ignored_task_num -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                mapCkpoint(getMyRuntime(), m2A, m2B);
            }
        });

        // thread 2: repeat ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should be empty.
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, true);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
        // Then verify they are empty.
        validateMapRebuild(mapSize, true);
    }

    /**
     * this test is similar to periodicCkpointTest(), but populating the maps is done BEFORE starting the checkpoint/recovery threads.
     * <p>
     * First, the test builds two maps, m2A m2B, and populates them with mapSize items.
     * <p>
     * Then, it brings up two threads:
     * <p>
     * 1. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 2. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread checks that all values are present in the maps
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    @Test
    public void periodicCkpointNoUpdatesTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // pre-populate map
        populateMaps(mapSize);

        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                mapCkpoint(getMyRuntime(), m2A, m2B);
            }
        });

        // repeated ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // this thread checks that all values are present in the maps
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, true);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true);
    }

    /**
     * this test is similar to periodicCkpointTest(), but adds simultaneous log prefix-trimming.
     * <p>
     * the test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
     * and immediately trims the log up to the checkpoint position.
     * 3. one repeats ITERATIONS_LOW starting a fresh runtime, and instantiating the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */

    @Test
    public void periodicCkpointTrimTest() throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(mapSize);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
        // and immediate prefix-trim of the log up to the checkpoint position
        scheduleConcurrently(1, ignored_task_num -> {
            long checkpointAddress = -1;
            CorfuRuntime currentRuntime = getMyRuntime();

            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                checkpointAddress = mapCkpoint(currentRuntime, m2A, m2B);
                try {
                    Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
                } catch (InterruptedException ie) {
                    //
                }
                // Trim the log
                logTrim(currentRuntime, checkpointAddress - 1);
            }
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true);
    }

    @Test
    public void periodicCkpointTrimLongevity() throws Exception {
        CorfuRuntime currentRuntime = getMyRuntime();

        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            for (;;) {
                currentRuntime.getObjectsView().TXBuild()
                        .setType(TransactionType.WRITE_AFTER_WRITE)
                        .begin();

                populateMaps(mapSize);
                currentRuntime.getObjectsView().TXEnd();
            }
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
        // and immediate prefix-trim of the log up to the checkpoint position
        scheduleConcurrently(1, ignored_task_num -> {
            long checkpointAddress = -1;
            CorfuRuntime currentRuntime2 = getMyRuntime();

            for (;;) {
                checkpointAddress = mapCkpoint(currentRuntime2, m2A, m2B);
                try {
                    Thread.sleep(PARAMETERS.TIMEOUT_NORMAL.toMillis());
                } catch (InterruptedException ie) {
                    //
                }
                // Trim the log
                System.out.println("trim " + checkpointAddress);
                if (checkpointAddress > 0)
                    logTrim(currentRuntime2, checkpointAddress - 1);
            }
        });

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_VERY_LOW, ignored_task_num -> {
                    CorfuRuntime currentRuntime2 = currentRuntime; //setNewRuntime();
                    Map<String, Long> localm2A = instantiateMap(currentRuntime2, streamNameA);
                    Map<String, Long> localm2B = instantiateMap(currentRuntime2, streamNameB);

                    for (int j = 0; ; j++) {
                        try {
                            currentRuntime2.getObjectsView().TXBuild()
                                    .setType(TransactionType.OPTIMISTIC)
                                    .begin();

                            System.out.println(j + ".." +
                                TransactionalContext.getCurrentContext().getSnapshotTimestamp());

                            for (int i = 0; i < localm2A.size(); i++) {
                                //System.out.println(j + "." + i + "..");
                                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
                            }

                            if (j % 2 == 0)
                                Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
                            else
                                Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());

                            for (int i = 0; i < localm2B.size(); i++) {
                                //System.out.println("#2" + j + "." + i + "..");
                                assertThat(localm2B.get(String.valueOf(i))).isEqualTo(0L);
                            }

                            System.out.println("commit " + j + "..");

                            currentRuntime2.getObjectsView().TXEnd();
                        } catch (InterruptedException ie) {
                            System.out.println(j + " ..interrupted");
                        } catch (TrimmedException te) {
                            System.out.println(j + " ..trimmed exception");
                        }
                    }
                });

        executeScheduled(PARAMETERS.CONCURRENCY_LOTS, Duration.ofHours(2));
    }
}
