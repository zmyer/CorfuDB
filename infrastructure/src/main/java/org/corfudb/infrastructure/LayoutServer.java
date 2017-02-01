package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.router.*;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 * <p>
 * For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 * <p>
 * 1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK_RESPONSE, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * If the server responds with LAYOUT_PREPARE_REJECT_ERROR, the server
 * informs the client of the current high rank and the request is
 * rejected.
 * <p>
 * 2)   Propose(rank,layout) - Clients then contact each server with
 * the previously prepared rank and the desired layout. If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT_ERROR,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 * <p>
 * 3)   Committed(rank, layout) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 * <p>
 * Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
public class LayoutServer extends AbstractPreconditionServer<CorfuMsg, CorfuMsgType, LayoutServer> {

    private static final String PREFIX_LAYOUT = "LAYOUT";
    private static final String KEY_LAYOUT = "CURRENT";
    private static final String PREFIX_PHASE_1 = "PHASE_1";
    private static final String KEY_SUFFIX_PHASE_1 = "RANK";
    private static final String PREFIX_PHASE_2 = "PHASE_2";
    private static final String KEY_SUFFIX_PHASE_2 = "DATA";
    private static final String PREFIX_LAYOUTS = "LAYOUTS";

    /**
     * The options map.
     */
    private final Map<String, Object> opts;

    @Getter
    private final ServerContext serverContext;

    /** Handler for the base server */
    @Getter
    private final PreconditionServerMsgHandler<CorfuMsg, CorfuMsgType, LayoutServer> preconditionMsgHandler =
            new PreconditionServerMsgHandler<CorfuMsg, CorfuMsgType, LayoutServer>(this)
                    .generateHandlers(MethodHandles.lookup(), this, ServerHandler.class, ServerHandler::type);

    public LayoutServer(IServerRouter<CorfuMsg, CorfuMsgType> router,
                        ServerContext serverContext) {

        // Setup base class and precondition
        super(router, (msg, channel, server) -> {
            // Precondition: If the message type is not BOOTSTRAP,
            // reject the message unless the server is already
            // bootstrapped.
            if (msg.getMsgType() != CorfuMsgType.LAYOUT_BOOTSTRAP)
            {
                if (server.getCurrentLayout() == null) {
                    server.sendResponse(channel, msg,
                            CorfuMsgType.LAYOUT_NOBOOTSTRAP_ERROR.msg());
                    return false;
                }
                return true;
            }
            return true;
        });
        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;

        if ((Boolean) opts.get("--single"))
            getSingleNodeLayout();
    }

    private void getSingleNodeLayout() {
        String localAddress = opts.get("--address") + ":" + opts.get("<port>");
        setCurrentLayout(new Layout(
                Collections.singletonList(localAddress),
                Collections.singletonList(localAddress),
                Collections.singletonList(new LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        Collections.singletonList(localAddress)
                                )
                        )
                )),
                0L
        ));
    }

    // Helper Methods
    @ServerHandler(type=CorfuMsgType.LAYOUT_REQUEST)
    public synchronized CorfuMsg handleMessageLayoutRequest(CorfuPayloadMsg<Long> msg, IChannel<CorfuMsg> channel) {
        long epoch = msg.getPayload();
        if (epoch <= serverContext.getServerEpoch()) {
            return new LayoutMsg(getCurrentLayout(), CorfuMsgType.LAYOUT_RESPONSE);
        } else {
            // else the client is somehow ahead of the server.
            //TODO figure out a strategy to deal with this situation
            long serverEpoch = serverContext.getServerEpoch();
            log.warn("Message Epoch {} ahead of Server epoch {}", epoch, serverContext.getServerConfig());
            return CorfuMsgType.WRONG_EPOCH_ERROR.payloadMsg(serverEpoch);
        }
    }

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg
     * @param channel
     */
    @ServerHandler(type=CorfuMsgType.LAYOUT_BOOTSTRAP)
    public CorfuMsg handleMessageLayoutBootstrap(CorfuPayloadMsg<LayoutBootstrapRequest> msg, IChannel<CorfuMsg> channel) {
        if (getCurrentLayout() == null) {
            log.info("Bootstrap with new layout={}, {}",  msg.getPayload().getLayout(), msg);
            setCurrentLayout(msg.getPayload().getLayout());
            serverContext.setServerEpoch(getCurrentLayout().getEpoch());
            //send a response that the bootstrap was successful.
            return CorfuMsgType.ACK_RESPONSE.msg();
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, rejecting!");
            return CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP_ERROR.msg();
        }
    }

    /** Respond to a epoch change message.
     *
     * @param msg      The incoming message
     * @param channel       The channel context
     */
    @ServerHandler(type=CorfuMsgType.SEAL_EPOCH)
    public synchronized CorfuMsg handleMessageSealEpoch(CorfuPayloadMsg<Long> msg,
                                                        IChannel<CorfuMsg> channel) {
        long serverEpoch = getServerEpoch();
        if (msg.getPayload() >= serverEpoch) {
            log.info("Received SEAL_EPOCH, moving to new epoch {}", msg.getPayload());
            setServerEpoch(msg.getPayload());
            return CorfuMsgType.ACK_RESPONSE.msg();
        } else {
            log.debug("Rejected SEAL_EPOCH currrent={}, requested={}", serverEpoch, msg.getPayload());
            return CorfuMsgType.WRONG_EPOCH_ERROR.msg();
        }
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     * @param msg
     * @param channel
     */
    // TODO this can work under a separate lock for this step as it does not change the global components
    @ServerHandler(type=CorfuMsgType.LAYOUT_PREPARE)
    public CorfuMsg handleMessageLayoutPrepare(CorfuPayloadMsg<LayoutPrepareRequest> msg, IChannel<CorfuMsg> channel) {
        // Check if the prepare is for the correct epoch
        Rank prepareRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        Rank phase1Rank = getPhase1Rank();
        Layout proposedLayout = getProposedLayout();

        long serverEpoch = getServerEpoch();
        if (msg.getPayload().getEpoch() != serverEpoch) {
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}", msg.getEpoch(), serverEpoch, msg);
            return CorfuMsgType.WRONG_EPOCH_ERROR.payloadMsg(serverEpoch);
        }


        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (phase1Rank != null && prepareRank.lessThanEqualTo(phase1Rank)) {
            log.debug("Rejected phase 1 prepare of rank={}, phase1Rank={}", prepareRank, phase1Rank);
            return CorfuMsgType.LAYOUT_PREPARE_REJECT_ERROR
                    .payloadMsg(new LayoutPrepareResponse(phase1Rank.getRank(), proposedLayout));
        } else {
            setPhase1Rank(prepareRank);
            log.debug("New phase 1 rank={}", getPhase1Rank());
            return CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE
                    .payloadMsg(new LayoutPrepareResponse(prepareRank.getRank(), proposedLayout));
        }
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     * @param msg
     * @param channel
     */
    @ServerHandler(type=CorfuMsgType.LAYOUT_PROPOSE)
    public CorfuMsg handleMessageLayoutPropose(CorfuPayloadMsg<LayoutProposeRequest> msg, IChannel<CorfuMsg> channel) {
        // Check if the propose is for the correct epoch
        Rank proposeRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        Layout proposeLayout = msg.getPayload().getLayout();
        Rank phase1Rank = getPhase1Rank();
        Rank phase2Rank = getPhase2Rank();

        long serverEpoch = getServerEpoch();

        if (msg.getPayload().getEpoch() != serverEpoch) {
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}", proposeLayout.getEpoch(), serverEpoch, msg);
            return CorfuMsgType.WRONG_EPOCH_ERROR.payloadMsg(serverEpoch);
        }
        // This is a propose. If no prepare, reject.
        if (phase1Rank == null) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank=none", proposeRank);
            return CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR.payloadMsg(new LayoutProposeResponse(-1));
        }
        // This is a propose. If the rank is less than or equal to the phase 1 rank, reject.
        if (!proposeRank.equals(phase1Rank)) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank={}", proposeRank, phase1Rank);
            return CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR.payloadMsg(new LayoutProposeResponse(phase1Rank.getRank()));
        }
        // In addition, if the rank is equal to the current phase 2 rank (already accepted message), reject.
        // This can happen in case of duplicate messages.
        if (phase2Rank != null && proposeRank.equals(phase2Rank)) {
            log.debug("Rejected phase 2 propose of rank={}, phase2Rank={}", proposeRank, phase2Rank);
            return CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR.payloadMsg(new LayoutProposeResponse(phase2Rank.getRank()));
        }

        log.debug("New phase 2 rank={},  layout={}", proposeRank, proposeLayout);
        setPhase2Data(new Phase2Data(proposeRank, proposeLayout));
        return CorfuMsgType.ACK_RESPONSE.msg();
    }

    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     * @param msg
     * @param channel
     */
    // TODO If a server does not get SEAL_EPOCH layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if let in layout commit message. Maybe we have a hole filling process
    // TODO how do reject the older epoch commits, should it be an explicit NACK_ERROR.
    @ServerHandler(type=CorfuMsgType.LAYOUT_COMMITTED)
    public synchronized CorfuMsg handleMessageLayoutCommit(CorfuPayloadMsg<LayoutCommittedRequest> msg, IChannel<CorfuMsg> channel) {
        Layout commitLayout = msg.getPayload().getLayout();
        long serverEpoch = getServerEpoch();
        if(msg.getPayload().getEpoch() < serverEpoch) {
            return CorfuMsgType.WRONG_EPOCH_ERROR.payloadMsg(serverEpoch);
        }

        setCurrentLayout(commitLayout);
        setServerEpoch(msg.getPayload().getEpoch());
        return CorfuMsgType.ACK_RESPONSE.msg();
    }


    public Layout getCurrentLayout() {
        return serverContext.getDataStore().get(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT);
    }

    public void setCurrentLayout(Layout layout) {
        serverContext.getDataStore().put(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT, layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public Rank getPhase1Rank() {
        return serverContext.getDataStore().get(Rank.class, PREFIX_PHASE_1, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_1);
    }

    public void setPhase1Rank(Rank rank) {
        serverContext.getDataStore().put(Rank.class, PREFIX_PHASE_1, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_1, rank);
    }

    public Phase2Data getPhase2Data() {
        return serverContext.getDataStore().get(Phase2Data.class, PREFIX_PHASE_2, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_2);
    }

    public void setPhase2Data(Phase2Data phase2Data) {
        serverContext.getDataStore().put(Phase2Data.class, PREFIX_PHASE_2, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_2, phase2Data);
    }

    public void setLayoutInHistory(Layout layout) {
        serverContext.getDataStore().put(Layout.class, PREFIX_LAYOUTS, String.valueOf(layout.getEpoch()), layout);
    }

    private void setServerEpoch(long serverEpoch) {
        serverContext.setServerEpoch(serverEpoch);
    }

    private long getServerEpoch() {
        return serverContext.getServerEpoch();
    }

    public List<Layout> getLayoutHistory() {
        List<Layout> layouts = serverContext.getDataStore().getAll(Layout.class, PREFIX_LAYOUTS);
        Collections.sort(layouts, (a, b) -> {
            if (a.getEpoch() > b.getEpoch()) {
                return 1;
            } else if (a.getEpoch() < b.getEpoch()) {
                return -1;
            } else {
                return 0;
            }
        });
        return layouts;
    }

    public Rank getPhase2Rank() {
        Phase2Data phase2Data = getPhase2Data();
        if (phase2Data != null) {
            return phase2Data.getRank();
        }
        return null;
    }

    public Layout getProposedLayout() {
        Phase2Data phase2Data = getPhase2Data();
        if (phase2Data != null) {
            return phase2Data.getLayout();
        }
        return null;
    }

    protected void finalize() {
        //
    }
}
