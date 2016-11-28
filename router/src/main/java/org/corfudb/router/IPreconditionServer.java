package org.corfudb.router;

/** Interface for a precondition server, which only accepts messages
 * which pass a given precondition.
 * @param <M> The type of the messages this server accepts.
 * @param <T> The type of the message type for this server.
 * @param <S> The type of the implementing server.
 * Created by mwei on 11/27/16.
 */
public interface IPreconditionServer<M extends IRoutableMsg<T>, T,
        S extends IPreconditionServer<M, T, S>>
        extends IServer<M, T> {

    /** A functional interface for a precondition function,
     * which evaluates incoming messages before they are processed
     * by the server.
     * @param <M>   The type of messages this server accepts.
     * @param <T>   The type of the message type for this server.
     * @param <S>   The type of the implementing server.
     */
    @FunctionalInterface
    interface PreconditionFunction<M extends IRoutableMsg<T>, T,
            S extends IPreconditionServer<M, T, S>> {
        /** Check whether a given message should be accepted by this server.
         * @param message   The message to check.
         * @param channel   The channel the message came from.
         * @param server    The instance of the server itself.
         * @return          True, if the message should be accepted by the
         *                  server.
         */
        boolean check(M message, IChannel<M> channel, S server);
    }

    /** Retrieve the precondition function for this server.
     * @return  The precondition function for this server.
     */
    PreconditionFunction<M, T, S> getPreconditionFunction();
}
