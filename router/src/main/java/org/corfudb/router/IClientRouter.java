package org.corfudb.router;

import java.util.function.Function;

/** An interface for client routers, which route messages from servers to
 * clients.
 * @param <M> The type of messages this client router handles.
 * @param <T> The type of the type of messages this client router handles.
 * Created by mwei on 11/23/16.
 */
public interface IClientRouter<M extends IRoutableMsg<T>, T>
        extends AutoCloseable {

    /** Retrieve the first client of a given type registered to the router.
     * @param clientType    The class of the client to retrieve.
     * @param <C>           The type of the client to retrieve.
     * @return The client of the type requested, or null, if there is no
     * client of that type registered. */
    <C extends IClient<M, T>> C getClient(Class<C> clientType);

    /** Register a client, through a function which is provided with
     * this router and must supply a new client, like a factory method.
     * @param clientSupplier    A function which supplies new clients.
     * @return                  This router, to support chaining.
     */
    default IClientRouter<M, T> registerClient(
            final Function<IClientRouter<M, T>, IClient<M, T>> clientSupplier) {
        return registerClient(clientSupplier.apply(this));
    }

    /**
     * Register a client with this router.
     * @param client    The client to register.
     * @return          This router, to support chaining.
     */
    IClientRouter<M, T> registerClient(IClient<M, T> client);

    /** Asynchronously send a message to the server.
     * @param msg       The message to send. */
    void sendMessage(M msg);

    /** Connects to a server endpoint and starts routing client
     * requests, as well as server messages to clients.
     * @return          This router, to support chaining.
     */
    IClientRouter<M, T> start();

    /** Disconnects from a server endpoint and stops routing client
     * requests, as well as server messages to clients.
     * @return          This router, to support chaining.
     */
    IClientRouter<M, T> stop();

    /**
     * {@inheritDoc}
     */
    @Override
    default void close() throws Exception {
        stop();
    }
}
