package org.corfudb.runtime.clients;

import com.google.common.reflect.TypeToken;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FailureDetectorMsg;
import org.corfudb.router.AbstractRequestClient;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.runtime.view.Layout;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A client to the Management Server.
 * <p>
 * Failure Detection:
 * This client allows a client to trigger failures handlers with relevant failures.
 * <p>
 * Created by zlokhandwala on 11/4/16.
 */
public class ManagementClient extends AbstractRequestClient<CorfuMsg, CorfuMsgType> {

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler<CorfuMsg,CorfuMsgType> msgHandler =
            new ClientMsgHandler<CorfuMsg,CorfuMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            ClientHandler.class, ClientHandler::type);


    public ManagementClient(IRequestClientRouter<CorfuMsg, CorfuMsgType> router) {
        super(router);
    }

    /**
     * Bootstraps a management server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     * bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapManagement(Layout l) {
        return sendMessageAndGetResponse(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(l))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    /**
     * Sends the failure detected to the relevant management server.
     *
     * @param nodes The failed nodes map to be handled.
     * @return A future which will be return TRUE if completed successfully else returns FALSE.
     */
    public CompletableFuture<Boolean> handleFailure(Map nodes) {
        return sendMessageAndGetResponse(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED
                .payloadMsg(new FailureDetectorMsg(nodes)))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    /**
     * Initiates failure handling in the Management Server.
     *
     * @return A future which returns TRUE if failure handler triggered successfully.
     */
    public CompletableFuture<Boolean> initiateFailureHandler() {
        return sendMessageAndGetResponse(CorfuMsgType.MANAGEMENT_START_FAILURE_HANDLER.msg())
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    /**
     * Requests for a heartbeat message containing the node status.
     *
     * @return A future which will return the node health metrics of
     * the node which was requested for the heartbeat.
     */
    public CompletableFuture<byte[]> sendHeartbeatRequest() {
        return sendMessageAndGetResponse(CorfuMsgType.HEARTBEAT_REQUEST.msg(),
                new TypeToken<CorfuPayloadMsg<byte[]>>() {})
                .thenApply(CorfuPayloadMsg::getPayload);
    }
}
