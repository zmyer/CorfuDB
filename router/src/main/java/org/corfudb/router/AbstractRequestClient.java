package org.corfudb.router;

import com.google.common.reflect.TypeToken;

import java.util.concurrent.CompletableFuture;

/** A basic implementation of a request client, which makes requests that
 * servers may respond to.
 * @param <M>   The type of the message, which must be routable.
 * @param <T>   The type of the message type.
 * Created by mwei on 11/25/16.
 */
public abstract class AbstractRequestClient<M extends IRoutableMsg<T>
        & IRespondableMsg, T extends IRespondableMsgType>
        extends AbstractClient<M, T> {
    /** Get a new client, given a router.
     * @param router    The client router this client should use.
     */
    public AbstractRequestClient(final IRequestClientRouter<M, T> router) {
        super(router);
    }

    /** Get the request router assigned to this client.
     *
     * @return  The request router assigned to this client.
     */
    public IRequestClientRouter<M, T> getRequestRouter() {
        return (IRequestClientRouter<M, T>) getRouter();
    }

    /** Send a message and wait for the response to be returned
     * as a completable future.
     * @param outMsg    The message to send
     * @return          A completable future which will be completed with
     *                  the response, or completed exceptionally if there
     *                  was a communication error.
     */
    protected CompletableFuture<M> sendMessageAndGetResponse(final M outMsg) {
        return getRequestRouter().sendMessageAndGetResponse(outMsg);
    }

    /** Send a message and get the response as a completable future,
     * setting the lower bound for the return type.
     * @param outMsg        The message to send.
     * @param responseType  The class of the type of the response message.
     * @param <R>           The lower bound for the response message type.
     * @return              A completed future which will be completed with
     *                      the response, or completed exceptionally if there
     *                      was a communication error.
     */
    protected <R extends M> CompletableFuture<R>
    sendMessageAndGetResponse(final M outMsg, final Class<R> responseType) {
        return (CompletableFuture<R>) sendMessageAndGetResponse(outMsg);
    }

    /** Send a message and get the response as a completable future,
     * setting the lower bound for the return type using a type token.
     * @param outMsg        The message to send.
     * @param typeToken    The class of the type of the response message.
     * @param <R>           The lower bound for the response message type.
     * @return              A completed future which will be completed with
     *                      the response, or completed exceptionally if there
     *                      was a communication error.
     */
    protected <R extends M> CompletableFuture<R>
    sendMessageAndGetResponse(final M outMsg, final TypeToken<R> typeToken) {
        return (CompletableFuture<R>) sendMessageAndGetResponse(outMsg);
    }
}
