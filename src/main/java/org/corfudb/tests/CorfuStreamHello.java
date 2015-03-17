package org.corfudb.tests;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;

import org.corfudb.client.OutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
/**
 * Created by dmalkhi on 1/16/15.
 */
public class CorfuStreamHello {

    private static final Logger log = LoggerFactory.getLogger(CorfuHello.class);

    /**

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String masteraddress = null;

        if (args.length >= 1) {
            masteraddress = args[0]; // TODO check arg.length
        } else {
            // throw new Exception("must provide master http address"); // TODO
            masteraddress = "http://localhost:8002/corfu";
        }

        final int numthreads = 1;

        CorfuDBClient client = new CorfuDBClient(masteraddress);
        client.startViewManager();

        UUID streamID = UUID.randomUUID();
        try (Stream s = new Stream(client, streamID)) {
        log.info("Appending hello world into log...");
        long address = 0;
        try {
            address = s.append("hello world from stream " + streamID.toString());
        }
        catch (OutOfSpaceException oose)
        {
            log.error("Out of space during append!", oose);
            System.exit(1);
        }
        log.info("Successfully appended hello world into log position " + address + ", stream "+ streamID.toString());
        log.info("Reading back entry at address " + address);
        String sresult = (String) s.readNextObject();
        log.info("Contents were: " + sresult);
        if (!sresult.toString().equals("hello world from stream " + streamID.toString()))
                {
                    log.error("ASSERT Failed: String did not match!");
                    System.exit(1);
                }
    }
        log.info("Successfully completed test!");
        System.exit(0);

    }
}
