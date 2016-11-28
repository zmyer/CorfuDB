package org.corfudb.router;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.util.function.Function;

/** This class automatically routes messages to methods on an IServer
 * which are annotated with the @ServerHandler(msgtype) annotation.
 * @param <M>   The type of message this handeler handles.
 * @param <T>   The type of the message type.
 * Created by mwei on 11/23/16.
 */
public class ServerMsgHandler<M extends IRoutableMsg<T>, T>
    extends AbstractMsgHandler<M, T> {

    /** The server we are handling messages for. */
    private final IServer<M, T> server;

    /** Construct a new instance of ServerMsgHandler.
     * @param handleServer The server to handle messages for. */
    public ServerMsgHandler(final IServer<M, T> handleServer) {
        this.server = handleServer;
    }

    /** Handle an incoming routable message.
     *
     * @param message   The message to handle.
     * @param ctx       The channel handler context.
     * @return          True, if the message was handled.
     *                  False otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean handle(final M message, final IChannel<M> ctx) {
        if (getHandlerMap().containsKey(message.getMsgType())) {
            M ret = getHandlerMap().get(message.getMsgType())
                    .handle(message, ctx);
            if (ret != null) {
                if (message instanceof IRespondableMsg) {
                    server.getRouter().sendResponse(ctx,
                            (IRespondableMsg) message,
                            (IRespondableMsg) ret);
                }
            }
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
    public ServerMsgHandler<M, T> addHandler(final T messageType,
                                             final Handler handler) {
        super.addHandler(messageType, handler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A extends Annotation> ServerMsgHandler<M, T>
    generateHandlers(final MethodHandles.Lookup caller,
                     final Object o,
                     final Class<A> annotationType,
                     final Function<A, T> typeFromAnnoationFn) {
        super.generateHandlers(caller, o, annotationType, typeFromAnnoationFn);
        return this;
    }
}
