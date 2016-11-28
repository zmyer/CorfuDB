package org.corfudb.router;

import java.util.function.Function;

/** An interface to a server router, which routes messages from clients
 * to registered servers.
 * @param <M> The type of messages handled by this router.
 * @param <T> The type of the message type.
 * Created by mwei on 11/23/16.
 */
public interface IServerRouter<M extends IRoutableMsg<T>, T>
        extends AutoCloseable {

    /** Send a message, unsolicited.
     *
     * @param ctx       The context to send the response to.
     * @param outMsg    The outgoing message.
     */
    void sendMessage(IChannel<M> ctx, M outMsg);

    /** Send a message, in response to a message.
     *
     * @param ctx       The context to send the response to.
     * @param inMsg     The message we are responding to.
     * @param outMsg    The outgoing message.
     */
    void sendResponse(IChannel<M> ctx, IRespondableMsg inMsg,
                      IRespondableMsg outMsg);

    /** Register a new server to route messages to, given
     * a function which generates a server from this router.
     * @param serverSupplier    The function which supplies the server.
     * @return                  This server router, to support chaining.
     */
    default IServerRouter<M, T> registerServer(Function<IServerRouter<M, T>,
            IServer<M, T>> serverSupplier) {
        return registerServer(serverSupplier.apply(this));
    }

    /** Register a new server to route messages to.
     * @param server    The server to route messages to.
     * @return          This server router, to support chaining.
     */
    IServerRouter<M, T> registerServer(IServer<M, T> server);

    /** Return a registered server of the given type.
     * @param serverType    The type of the server to return.
     * @param <R>           The type of the returned server.
     * @return              A server of the given type.
     */
    <R extends IServer<M, T>> R getServer(Class<R> serverType);

    /** Start listening for messages and route to the registered servers.
     *  @return        This server router, to support chaining.
     */
    IServerRouter<M, T> start();

    /** Stop listening for messages and stop routing messages to the
     *  registered servers.
     *  @return        This server router, to support chaining.
     */
    IServerRouter<M, T> stop();

    /**
     * {@inheritDoc}
     */
    @Override
    default void close() throws Exception {
        stop();
    }
}
