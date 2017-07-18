package org.corfudb.runtime.checkpoint;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;

import java.util.Map;

/**
 * Created by dalia on 7/16/17.
 */
public class AbstractCheckpointTest extends AbstractObjectTest {
    @Getter
    CorfuRuntime myRuntime = null;

    @Before
    public void initRuntime() {
        myRuntime = getDefaultRuntime().connect();
    }

    public CorfuRuntime setNewRuntime() {

        return (myRuntime = new CorfuRuntime(getDefaultConfigurationString()).connect());
    }

    public SMRMap<String, Long> instantiateMap(CorfuRuntime rt, String mapName) {
        final byte serializerByte = (byte) 20;
        ISerializer serializer = new CPSerializer(serializerByte);
        Serializers.registerSerializer(serializer);
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        rt,
                        new TypeToken<SMRMap<String, Long>>() {},
                        mapName,
                        serializer);
    }

    /**
     * checkpoint the parameter maps
     */
    public long mapCkpoint(CorfuRuntime rt, SMRMap... maps) throws Exception {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (SMRMap m : maps)
            mcw1.addMap((SMRMap) m);
        return mcw1.appendCheckpoints(rt, "dahlia");
    }


    /**
     * trim the log
     */
    public void logTrim(CorfuRuntime rt, long address) {
        rt.getAddressSpaceView().prefixTrim(address);
        rt.getAddressSpaceView().gc();
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
    }
}
