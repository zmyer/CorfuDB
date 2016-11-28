package org.corfudb.router;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.util.function.Function;

/** An annotation-driven handler which handles messages for clients.
 * @param <M>   The message type, which must be routable.
 * @param <T>   The type of the message type.
 * Created by mwei on 11/23/16.
 */
public class ClientMsgHandler<M extends IRoutableMsg<T>, T>
    extends AbstractMsgHandler<M, T> {

    /** The client. */
    private final AbstractClient client;

    /** Construct a new instance of ClientMsgHandler.
     * @param clientToHandle The client we are handling messages for. */
    public ClientMsgHandler(final AbstractClient clientToHandle) {
        this.client = clientToHandle;
    }

    /** Handle an incoming message.
     *
     * @param message   The message to handle.
     * @param channel   The channel handler context.
     * @return          True, if the message was handled.
     *                  False otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean handle(final M message, final IChannel<M> channel) {
        if (getHandlerMap().containsKey(message.getMsgType())) {
            getHandlerMap().get(message.getMsgType()).handle(message, channel);
            return true;
        }
        return false;
    }


    /**
     * Add a handler to this message handler.
     *
     * @param messageType The type of CorfuMsg this handler will handle.
     * @param handler     The handler itself.
     * @return This handler, to support chaining.
     */
    @Override
    public AbstractMsgHandler<M, T> addHandler(final T messageType,
                                               final Handler handler) {
        super.addHandler(messageType, handler);
        return this;
    }

    /**
     * Generate handlers for a particular client.
     *
     * @param caller              The context that is being used. Call
     *                            MethodHandles.lookup() to obtain.
     * @param o                   The object that implements the client.
     * @param annotationType      The type of the annotation to use.
     * @param typeFromAnnotationFn A function to obtain the type from the
     *                            annotation function.
     * @return                    Return this client message handler, for
     *                            chaining.
     */
    @Override
    public <A extends Annotation> ClientMsgHandler<M, T>
        generateHandlers(final MethodHandles.Lookup caller,
            final Object o, final Class<A> annotationType,
                         final Function<A, T> typeFromAnnotationFn) {
        super.generateHandlers(caller, o, annotationType, typeFromAnnotationFn);
        return this;
    }


}
