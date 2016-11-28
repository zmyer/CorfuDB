package org.corfudb.router;


import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.util.function.Function;

/** A precondition server message handler is a special case handler
 * for precondition servers.
 * @param <M> The type of messages this handler should handle.
 * @param <T> The type of the message type
 * @param <S> The type of the implementing server.
 * Created by mwei on 11/27/16.
 */
public class PreconditionServerMsgHandler<M extends IRoutableMsg<T>,
        T, S extends IPreconditionServer<M, T, S>> extends
    ServerMsgHandler<M, T> {

    /** The server to handle messages for. */
    private final S preconditionServer;

    /** Construct a new instance of ServerMsgHandler.
     * @param server The server to handle messages for.
     * */
    public PreconditionServerMsgHandler(final S server) {
        super(server);
        this.preconditionServer = server;
    }

    /**
     * Handle an incoming routable message.
     *
     * @param message The message to handle.
     * @param ctx     The channel handler context.
     * @return True, if the message was handled.
     * False otherwise.
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean handle(final M message, final IChannel<M> ctx) {
        // check if the message meets the precondition first.
        if (preconditionServer.getPreconditionFunction().check(message,
               ctx, preconditionServer)) {
            return super.handle(message, ctx);
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A extends Annotation> PreconditionServerMsgHandler<M, T, S>
    generateHandlers(final MethodHandles.Lookup caller, final Object o,
                     final Class<A> annotationType,
                     final Function<A, T> typeFromAnnotation) {
        super.generateHandlers(caller, o, annotationType, typeFromAnnotation);
        return this;
    }
}
