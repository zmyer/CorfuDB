package org.corfudb.runtime.recovery;

import org.corfudb.protocols.wireprotocol.ILogData;

import java.util.Map;

/**
 * Created by maithem on 6/21/17.
 */
public class Segment {
    public static Segment END = new Segment(null);
    public Map<Long, ILogData> addresses;
    public Segment(Map<Long, ILogData> addresses) {
        this.addresses = addresses;
    }
}