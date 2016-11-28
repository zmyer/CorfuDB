package org.corfudb.router;

import lombok.Getter;
import java.util.Set;

/** A basic implementation of a client which supports routable messages.
 *
 * Implement getMsgHandler by providing a message handler for your client.
 *
 * @param <M> The message type, which must be routable.
 * @param <T> The type of the message type.
 *
 * Created by mwei on 11/23/16.
 */
public abstract class AbstractClient<M extends IRoutableMsg<T>, T>
        implements IClient<M, T> {

    /** The underlying router for this client. */
    @Getter
    private final IClientRouter<M, T> router;

    /** Create a new client, given the router.
     * @param clientRouter The router for this client.
     */
    public AbstractClient(final IClientRouter<M, T> clientRouter) {
        this.router = clientRouter;
    }

    /** Get the client message handler for this client.
     * @return The client message handler.
     */
    public abstract ClientMsgHandler<M, T> getMsgHandler();

    /** Send a message, not expecting a response.
     *
     * @param msg   The message to send.
     */
    protected void sendMessage(final M msg) {
        router.sendMessage(msg);
    }

    /**
     * Handle a incoming message on the channel.
     *
     * @param msg     The incoming message
     * @param channel The channel
     */
    public void handleMessage(final M msg, final IChannel<M> channel) {
        getMsgHandler().handle(msg, channel);
    }

    /**
     * Returns a set of message types that the client handles.
     *
     * @return The set of message types this client handles.
     */
    public Set<T> getHandledTypes() {
        return getMsgHandler().getHandledTypes();
    }
}
