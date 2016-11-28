package org.corfudb.runtime.clients;

import com.google.common.reflect.TypeToken;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.router.AbstractRequestClient;
import org.corfudb.router.ClientMsgHandler;
import org.corfudb.router.IRequestClientRouter;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

/**
 * A client to the layout server.
 * <p>
 * In addition to being used by clients to obtain the layout and to report errors,
 * The layout client is also used by layout servers to initiate a Paxos-based protocol
 * for determining the next layout.
 * </p>
 * Created by mwei on 12/9/15.
 */
public class LayoutClient extends AbstractRequestClient<CorfuMsg, CorfuMsgType> {

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler<CorfuMsg,CorfuMsgType> msgHandler =
            new ClientMsgHandler<CorfuMsg,CorfuMsgType>(this)
                    .generateHandlers(MethodHandles.lookup(), this,
                            ClientHandler.class, ClientHandler::type);

    public LayoutClient(IRequestClientRouter<CorfuMsg, CorfuMsgType> router) {
        super(router);
    }

    /**
     * Retrieves the layout from the endpoint, asynchronously.
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        // Why does this request need the epoch?
        return sendMessageAndGetResponse(CorfuMsgType.LAYOUT_REQUEST.payloadMsg(0L), LayoutMsg.class)
                .thenApply(LayoutMsg::getLayout);
    }

    /**
     * Bootstraps a layout server.
     * @param l     The layout to bootstrap with.
     * @return      A completable future which will return TRUE if the
     *              bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean>  bootstrapLayout(Layout l)
    {
        return sendMessageAndGetResponse(CorfuMsgType.LAYOUT_BOOTSTRAP
                .payloadMsg(new LayoutBootstrapRequest(l)))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    /**
     * Begins phase 1 of a Paxos round with a prepare message.
     * @param epoch epoch for which the paxos rounds are being run
     * @param rank  The rank to use for the prepare.
     * @return      True, if the prepare was successful.
     *              Otherwise, the completablefuture completes exceptionally
     *              with OutrankedException.
     */
    public CompletableFuture<LayoutPrepareResponse> prepare(long epoch, long rank)
    {
        return sendMessageAndGetResponse(CorfuMsgType.LAYOUT_PREPARE
                        .payloadMsg(new LayoutPrepareRequest(epoch, rank)),
                new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>() {})
                .thenApply(CorfuPayloadMsg::getPayload);
    }

    /**
     * Begins phase 2 of a Paxos round with a propose message.
     * @param epoch     epoch for which the paxos rounds are being run
     * @param rank      The rank to use for the propose. It should be the same
     *                  rank from a successful prepare (phase 1).
     * @param layout    The layout to install for phase 2.
     * @return          True, if the propose was successful.
     *                  Otherwise, the completablefuture completes exceptionally
     *                  with OutrankedException.
     */
    public CompletableFuture<Boolean> propose(long epoch, long rank, Layout layout)
    {
        // TODO: return previous
        return sendMessageAndGetResponse(CorfuMsgType.LAYOUT_PROPOSE
                .payloadMsg(new LayoutProposeRequest(epoch, rank, layout)))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

    /**
     * Informs the server that the proposal (layout) has been committed to a quorum.
     * @param epoch epoch affiliated with the layout.
     * @param layout
     * @return True, if the commit was successful.
     */
    public CompletableFuture<Boolean> committed(long epoch, Layout layout)
    {
        return sendMessageAndGetResponse(CorfuMsgType.LAYOUT_COMMITTED
                .payloadMsg(new LayoutCommittedRequest(epoch, layout)))
                .thenApply(x -> x.getMsgType() == CorfuMsgType.ACK_RESPONSE);
    }

}
