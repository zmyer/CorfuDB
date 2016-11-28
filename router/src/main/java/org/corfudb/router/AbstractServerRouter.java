package org.corfudb.router;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** A basic implementation of a server router.
 * @param <M> The type of messages this server router handles.
 * @param <T> The type of the message type.
 * Created by mwei on 12/6/16.
 */
@Slf4j
public abstract class AbstractServerRouter<M extends IRoutableMsg<T>, T>
        implements IServerRouter<M, T> {
    /**
     * This map stores the mapping from message type to server handler.
     */
    private final Map<T, IServer<M, T>> handlerMap =
            new ConcurrentHashMap<>();

    /**
     * Add a new netty server handler to the router.
     * @param server The server to add.
     */
    @Override
    public AbstractServerRouter<M, T> registerServer(
            final IServer<M, T>  server) {
        server.getMsgHandler().getHandledTypes()
                .forEach(x -> {
                    handlerMap.put(x, server);
                });
        return this;
    }


    /**
     * Return a registered server of the given type.
     * @param serverType The type of the server to return.
     * @return A server of the given type.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R extends IServer<M, T>> R getServer(final Class<R> serverType) {
        Optional<IServer<M, T>> ret =  handlerMap.values()
                .stream()
                .filter(serverType::isInstance)
                .findFirst();
        return ret.isPresent() ? (R) ret.get() : null;
    }

    /**
     * Send a message, unsolicited.
     *
     * @param channel    The context to send the response to.
     * @param outMsg The outgoing message.
     */
    @Override
    public void sendMessage(final IChannel<M> channel, final M outMsg) {
        channel.sendMessage(outMsg);
    }

    /**
     * Send a message, in response to a message.
     *
     * @param channel    The context to send the response to.
     * @param inMsg  The message we are responding to.
     * @param outMsg The outgoing message.
     */
    @Override
    public void sendResponse(final IChannel<M> channel,
                             final IRespondableMsg inMsg,
                             final IRespondableMsg outMsg) {
        inMsg.copyFieldsToResponse(outMsg);
        sendMessage(channel, (M) outMsg);
    }

    /** Handle an incoming message using a registered server.
     *
     * @param channel       The channel the message came from.
     * @param message   Thhe message to process.
     */
    public void handleMessage(final IChannel<M> channel, final M message) {
        IServer<M, T> handler = handlerMap.get(message.getMsgType());
        if (handler == null) {
            // The message was unregistered, we are dropping it.
            log.warn("Received unregistered message {}, dropping", message);
        } else {
            handler.handleMessage(message, channel);
        }
    }

}
