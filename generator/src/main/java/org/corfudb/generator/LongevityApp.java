package org.corfudb.generator;

import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.runtime.CorfuRuntime;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by rmichoud on 7/25/17.
 */

/**
 * This Longevity app will stress test corfu using the Load
 * generator during a given duration.
 *
 * Current implementation spawns a task producer and a task
 * consumer. Both of them are a single threaded. In combination
 * with the blocking queue, they limit the amount of task created
 * during execution. Apps thread pool will take care of executing
 * the operations.
 *
 */
public class LongevityApp {

    private long durationMs;

    public LongevityApp(long durationMs) {
        this.durationMs = durationMs;
    }

    public void runLongevityTest() {

        BlockingQueue<Future> appsFutures = new ArrayBlockingQueue<Future>(1000);
        long startTime = System.currentTimeMillis();

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        State state = new State(50, 100, rt);

        Runnable task = () -> {
            try {
                Operation current = (Operation) state.getOperations().getRandomOperation();

                current.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        ExecutorService taskProducer = Executors.newSingleThreadExecutor();
        ExecutorService taskConsumer = Executors.newSingleThreadExecutor();

        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(cpTrimTask, 30, 20, TimeUnit.SECONDS);

        ExecutorService apps = Executors.newFixedThreadPool(10);

        taskProducer.execute(() -> {
            while(System.currentTimeMillis() - startTime < durationMs) {
                try {
                    appsFutures.put(apps.submit(task));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        taskConsumer.execute(() -> {
            while (System.currentTimeMillis() - startTime < durationMs || !appsFutures.isEmpty()) {
                if (!appsFutures.isEmpty()) {
                    Future f = appsFutures.poll();
                    try {
                        f.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            scheduler.shutdownNow();
            apps.shutdown();
        });

        taskProducer.shutdown();
        taskConsumer.shutdown();
    }
}
