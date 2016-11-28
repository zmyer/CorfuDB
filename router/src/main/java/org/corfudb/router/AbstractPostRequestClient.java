package org.corfudb.router;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** A basic implementation of a client which transforms messages in a
 * pre-defined way before they are sent to the server.
 * @param <M> The type of the messages sent by this client.
 * @param <T> The type of the message type
 * Created by mwei on 11/28/16.
 */
public abstract class AbstractPostRequestClient
        <M extends IRoutableMsg<T> & IRespondableMsg,
                T extends IRespondableMsgType>
        extends AbstractRequestClient<M, T> {

    /** A function which transforms messages before they are emitted to the
     * server.
     */
    private final Function<M, M> postTransformationFunction;

    /** Create a new client.
     * @param router                        The router to use.
     * @param transformationFunction    The function to run before emitting
     *                                      a message to the server.
     */
    public AbstractPostRequestClient(final IRequestClientRouter<M, T> router,
                           final Function<M, M> transformationFunction) {
        super(router);
        this.postTransformationFunction = transformationFunction;
    }

    /** Send a message, not expecting a response.
     *
     * @param msg   The message to send.
     */
    @Override
    protected void sendMessage(final M msg) {
        getRouter().sendMessage(postTransformationFunction.apply(msg));
    }

    /**
     * Send a message and get the response as a completable future,
     * setting the lower bound for the return type.
     *
     * @param outMsg       The message to send.
     * @return A completed future which will be completed with
     * the response, or completed exceptionally if there
     * was a communication error.
     */
    @Override
    protected CompletableFuture<M> sendMessageAndGetResponse(final M outMsg) {
        return super.sendMessageAndGetResponse(postTransformationFunction
                .apply(outMsg));
    }
}
