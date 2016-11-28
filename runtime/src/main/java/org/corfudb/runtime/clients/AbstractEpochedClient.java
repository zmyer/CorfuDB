package org.corfudb.runtime.clients;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.router.AbstractPostRequestClient;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by mwei on 11/30/16.
 */
public abstract class AbstractEpochedClient extends AbstractPostRequestClient<CorfuMsg, CorfuMsgType> {

    public AbstractEpochedClient(IRequestClientRouter<CorfuMsg, CorfuMsgType> router,
                           CorfuRuntime runtime) {
        super(router, inMsg -> {
            inMsg.setEpoch(runtime.getLayoutView().getCurrentLayout().getEpoch());
            return inMsg;
        });
    }

}
