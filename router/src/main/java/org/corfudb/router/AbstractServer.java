package org.corfudb.router;

import lombok.Getter;

/** A basic implementation of a server.
 * @param <M> The type of the message, which must be routable.
 * @param <T> The type of the message type.
 * Created by mwei on 11/23/16.
 */
public abstract class AbstractServer<M extends IRoutableMsg<T>, T>
        implements IServer<M, T> {

    /** The router this server is attached to. */
    @Getter
    private final IServerRouter<M, T> router;

    /** Get the message handler for this server.
     * @return  The message handler for this server.
     */
    public abstract ServerMsgHandler<M, T> getMsgHandler();

    /** Get a new server, given a server router.
     * @param serverRouter  The server router to use.
     */
    public AbstractServer(final IServerRouter<M, T> serverRouter) {
        this.router = serverRouter;
    }

    /** Send a message, using the given channel.
     * @param channel   The channel to send the message on.
     * @param msg       The message to send.
     */
    public void sendMessage(final IChannel<M> channel, final M msg) {
        router.sendMessage(channel, msg);
    }

    /** Respond to a message, using the given channel.
     * @param channel       The channel to send the mssaage on.
     * @param inMsg     The incoming message (to respond to).
     * @param outMsg    The message to send in response.
     */
    public void sendResponse(final IChannel<M> channel,
                             final IRespondableMsg inMsg,
                             final IRespondableMsg outMsg) {
        router.sendResponse(channel, inMsg, outMsg);
    }
}
