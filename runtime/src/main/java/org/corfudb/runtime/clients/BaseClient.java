package org.corfudb.runtime.clients;

import com.google.common.reflect.TypeToken;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.router.*;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

/**
 * This is a base client which processes basic messages.
 * It mainly handles PINGs, as well as the ACK_RESPONSE/NACKs defined by
 * the Corfu protocol.
 * <p>
 * Created by mwei on 12/9/15.
 */
@Slf4j
public class BaseClient extends AbstractRequestClient<CorfuMsg, CorfuMsgType> {

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler<CorfuMsg,CorfuMsgType> msgHandler =
            new ClientMsgHandler<CorfuMsg,CorfuMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            ClientHandler.class, ClientHandler::type);

    public BaseClient(IRequestClientRouter<CorfuMsg, CorfuMsgType> router) {
        super(router);
    }

    /**
     * Ping the endpoint, synchronously.
     *
     * @return True, if the endpoint was reachable, false otherwise.
     */
    public boolean pingSync() {
        try {
            return ping().get();
        } catch (Exception e) {
            log.trace("Ping failed due to exception", e.getCause());
            return false;
        }
    }

    public CompletableFuture<VersionInfo> getVersionInfo() {
        return sendMessageAndGetResponse(CorfuMsgType.VERSION_REQUEST.msg(),
                new TypeToken<JSONPayloadMsg<VersionInfo>>() {})
                .thenApply(JSONPayloadMsg::getPayload);
    }


    /**
     * Ping the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint is reachable, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> ping() {
        return sendMessageAndGetResponse(CorfuMsgType.PING.msg())
                .thenApply(x -> x.getMsgType() == CorfuMsgType.PONG_RESPONSE);
    }

    /**
     * Reset the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint acks, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> reset() {
        return sendMessageAndGetResponse(
                CorfuMsgType.RESET.msg())
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    /** Handle a ping request from the server.
     *
     * @param msg       The ping request message
     * @param channel   The channel the message was sent under
     * @return      The return value, null since this is a message from the server.
     */
    @ClientHandler(type=CorfuMsgType.PING)
    private static Object handlePing(CorfuMsg msg, IChannel<CorfuMsg> channel) {
        return CorfuMsgType.PONG_RESPONSE.msg();
    }

}
