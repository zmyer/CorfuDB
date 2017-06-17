package org.corfudb.integration;

import com.google.gson.Gson;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.recovery.ParallelReader;
import org.corfudb.runtime.recovery.Segment;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT  {


    @Test
    public void testParallelRead() throws Exception {
        ParallelReader parallelReader = new ParallelReader(4, "192.168.1.67:9000");

        Future readerFt = parallelReader.read(0, 200000);

        long addressesProcessed = 0;
        long startTime = System.nanoTime();
        while (true) {
            Segment segment = parallelReader.getQueue().take().get();
            if (segment == Segment.END) {
                break;
            }


                //System.out.println("Processed address " + addressesProcessed);
            Thread.sleep(50);
            addressesProcessed += segment.addresses.size();
        }
        long stopTime = System.nanoTime();
        System.out.println("Elapsed time: " + (stopTime - startTime));

        System.out.println("Number of addresses processed: " + addressesProcessed);
    }

    @Test
    public void simpleStreamTest() throws Exception {


        Gson gson = new Gson();
        String result = gson.toJson(new Payload());
        byte[] dataJson = result.getBytes();

        Payload rev = gson.fromJson(result, Payload.class);

        UUID streamId = CorfuRuntime.getStreamID("s1");

        String configStr = "192.168.1.67:9000";


        int numThreads = 8 * 4;
        int iterations = 2000000 / numThreads;
        Thread[] threads = new Thread[numThreads];

        long ss1 = System.currentTimeMillis();

        for (int x = 0; x < numThreads; x++) {
            CorfuRuntime rt = new CorfuRuntime(configStr).connect();
            rt.setCacheDisabled(true);
            IStreamView s1 = rt.getStreamsView().get(streamId);

            threads[x] = new Thread(
                    new Runnable() {
                        public void run() {
                            for (int x = 0; x < iterations; x++) {
                                s1.append(dataJson);
                            }
                        }
                    });

            threads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            threads[x].join();
        }

        long ss2 = System.currentTimeMillis();
        System.out.print(ss2 - ss1);

        // Read back the data and verify it is correct
        //for(int x = 0; x < numEntries; x++) {
        // ILogData entry = s1.nextUpTo(x);
        //  Payload tmp =  (Payload)entry.getPayload(rt);

        // long ss3 = System.currentTimeMillis();


        /*
        CorfuRuntime rt = new CorfuRuntime(configStr).connect();
        long s1 = System.currentTimeMillis();
        long x = 0;
        try {
            for (x = 0; x < Long.MAX_VALUE; x++) {
                ILogData data = rt.getAddressSpaceView().read(x);
                byte[] buf = (byte[]) data.getPayload(rt);
                String doc2 = new String(buf);
                Payload obj = gson.fromJson(doc2, Payload.class);
                int a = 0;
                if(x % 10000 == 0) {
                    System.out.println("reading at" + x);
                }
            }
        } catch (RuntimeException e) {
            System.out.println("adddress is " + x);
        }

        long e1 = System.currentTimeMillis();
        System.out.println(e1 - s1);
        */
    }


    public class Payload {
        private long f1 = 1;
        private long f2 = 2;
        private long f4 = 3;
        private long f5 = 4;
        private long f6 = 5;

        private UUID f7 = new UUID(0, 0);
        private UUID f8 = new UUID(0, 0);
        private UUID f9 = new UUID(0, 0);
        private UUID f10 = new UUID(0, 0);
        private UUID f11 = new UUID(0, 0);

        private String f12 = "This is a string, This is a string, This is a string, This is a string, This is a string";
        private String f13 = "This is a string, This is a string, This is a string, This is a string, This is a string";
        private String f14 = "This is a string, This is a string, This is a string, This is a string, This is a string";
        private String f15 = "This is a string, This is a string, This is a string, This is a string, This is a string";
        private String f16 = "This is a string, This is a string, This is a string, This is a string, This is a string";

        private Set<String> set;
        private HashMap<String, String> map;

        public Payload() {
            set = new HashSet();
            map = new HashMap();

            for(int x = 0; x < 100; x++) {
                set.add("This is a string" + x);
                map.put("key" + x, "string value");
            }
        }

    }
}
