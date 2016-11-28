package org.corfudb.runtime.clients;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.runtime.CorfuRuntime;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A sequencer client.
 * <p>
 * This client allows the client to obtain sequence numbers from a sequencer.
 * <p>
 * Created by mwei on 12/10/15.
 */
public class SequencerClient extends AbstractEpochedClient {

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler<CorfuMsg,CorfuMsgType> msgHandler =
            new ClientMsgHandler<CorfuMsg,CorfuMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            ClientHandler.class, ClientHandler::type);

    public SequencerClient(IRequestClientRouter<CorfuMsg, CorfuMsgType> router,
                           CorfuRuntime runtime) {
        super(router, runtime);
    }


    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return sendMessageAndGetResponse(
                CorfuMsgType.TOKEN_REQUEST
                        .payloadMsg(new TokenRequest(numTokens, streamIDs, false, false)),
                new TypeToken<CorfuPayloadMsg<TokenResponse>>() {})
                        .thenApply(CorfuPayloadMsg::getPayload);
    }

    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens,
                                                      boolean overwrite,
                                                      boolean replexOverwrite) {
        return sendMessageAndGetResponse(
                CorfuMsgType.TOKEN_REQUEST
                        .payloadMsg(new TokenRequest(numTokens, streamIDs, overwrite, replexOverwrite)),
                new TypeToken<CorfuPayloadMsg<TokenResponse>>() {})
                        .thenApply(CorfuPayloadMsg::getPayload);
    }

    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens,
                                                      boolean overwrite,
                                                      boolean replexOverwrite,
                                                      boolean txnResolution,
                                                      long readTimestamp,
                                                      Set<UUID> readSet) {
        return sendMessageAndGetResponse(
                CorfuMsgType.TOKEN_REQUEST.payloadMsg(new TokenRequest(numTokens, streamIDs, overwrite, replexOverwrite,
                        txnResolution, readTimestamp, readSet)),
                new TypeToken<CorfuPayloadMsg<TokenResponse>>() {})
                .thenApply(CorfuPayloadMsg::getPayload);
    }
}
