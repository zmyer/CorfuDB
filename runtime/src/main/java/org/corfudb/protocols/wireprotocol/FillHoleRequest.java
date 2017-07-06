package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.UUID;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Request sent to fill a hole.
 * Created by Maithem on 10/13/2016.
 */
@Data
@RequiredArgsConstructor
public class FillHoleRequest implements ICorfuPayload<FillHoleRequest> {

    final UUID stream;
    final List<Long> addresses;

    public FillHoleRequest(long address){
        this(null, ImmutableList.of(address));
    }

    public FillHoleRequest(List<Long> addresses){
        this(null, addresses);
    }

    /**
     * Constructor to generate a Fill Hole Request Payload.
     *
     * @param buf The buffer to deserialize.
     */
    public FillHoleRequest(ByteBuf buf) {
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            stream = ICorfuPayload.fromBuffer(buf, UUID.class);
        } else {
            stream = null;
        }
        addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, stream != null);
        if (stream != null) {
            ICorfuPayload.serialize(buf, stream);
        }
        ICorfuPayload.serialize(buf, addresses);
    }
}