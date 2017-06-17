package org.corfudb.runtime.recovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * Created by maithem on 6/21/17.
 */
public class ParallelReader {
    private final BlockingQueue<Future<Segment>> queue;
    private final int singleReadBatchSize = 10;
    ThreadFactory producerTF = new ThreadFactoryBuilder()
            .setNameFormat("producer-%d").build();
    final ExecutorService producer = Executors.newSingleThreadExecutor(producerTF);
    final ExecutorService workers;
    final CorfuRuntime[] runtimes;
    final int numThreads;


    public ParallelReader(int numThreads, String config) {
        this.numThreads = numThreads;
        this.queue = new LinkedBlockingQueue(100);
        this.workers = Executors.newWorkStealingPool(numThreads);
        this.runtimes = new CorfuRuntime[numThreads];

        for (int x = 0; x < numThreads; x++) {
            runtimes[x] = new CorfuRuntime(config).connect();
        }
    }

    public BlockingQueue<Future<Segment>> getQueue() {
        return queue;
    }

    public Future read(long start, long end) {
        Runnable task = () -> {
            try {
                int index = 0;
                long a = start;
                while (a <= end) {
                    long endPoint = Math.min(a + singleReadBatchSize, end);
                    SegmentRequest sr = new SegmentRequest(a, endPoint);
                    a = endPoint + 1;
                    submitRead(sr, runtimes[index % numThreads]);
                    index++;
                }

                Future<Segment> endSegment = CompletableFuture.completedFuture(Segment.END);
                queue.put(endSegment);
            } catch (InterruptedException e) {
                throw new RuntimeException("InterruptedException caught in lambda", e);
            }
        };

        return producer.submit(task);
    }

    private void submitRead(SegmentRequest sr, CorfuRuntime rt) throws InterruptedException {
        Callable<Segment> task = () -> {
            HashSet<Long> range = new HashSet();
            for (long x = sr.start; x <= sr.end; x++) {
                range.add(x);
            }
            Map<Long, ILogData> addresses = rt.getAddressSpaceView().cacheFetch(range);
            for (long a : addresses.keySet()) {
                addresses.get(a).getPayload(rt);
            }
            return new Segment(addresses);
        };

        Future<Segment> ft = workers.submit(task);
        queue.put(ft);
    }

    public class SegmentRequest {
        public final long start;
        public final long end;

        public SegmentRequest(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}
