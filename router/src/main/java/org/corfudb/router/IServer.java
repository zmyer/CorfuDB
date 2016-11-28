package org.corfudb.router;

/** Represents a server which can process messages routed from clients.
 * @param <M>   The message type this server handles.
 * @param <T>   The type of the message type.
 * Created by mwei on 11/23/16.
 */
public interface IServer<M extends IRoutableMsg<T>, T> {

    /** Get the message handler for this instance.
     * @return  A message handler.
     */
    ServerMsgHandler<M, T> getMsgHandler();

    /**
     * Handle a incoming message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     */
    default void handleMessage(M msg, IChannel<M> ctx) {
        getMsgHandler().handle(msg, ctx);
    }

    /** Get the router this server is attached to.
     * @return The router this server is attached to. */
    IServerRouter<M, T> getRouter();

    /** Shutdown (stop) the server. */
    default void stop() { }

    /** Startup the server. */
    default void start() { }
}
