package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Response for bulk hole fill
 * Created by zjohnny on 6/30/17.
 */
@Data
@AllArgsConstructor
public class FillHoleResponse implements ICorfuPayload<FillHoleResponse> {

    @Getter
    private Map<Long, Byte> result;

    //for value adopted exception
    @Getter
    private ReadResponse valueAdopted;

    public FillHoleResponse(){
        result = new LinkedHashMap<>();
    }

    public FillHoleResponse(ByteBuf buf) {
        result = ICorfuPayload.mapFromBuffer(buf, Long.class, Byte.class);
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            valueAdopted = ICorfuPayload.fromBuffer(buf, ReadResponse.class);
        } else {
            valueAdopted = null;
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, result);
        ICorfuPayload.serialize(buf, valueAdopted != null);
        if (valueAdopted != null) {
            ICorfuPayload.serialize(buf, valueAdopted);
        }
    }
}
