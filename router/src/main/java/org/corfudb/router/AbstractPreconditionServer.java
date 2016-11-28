package org.corfudb.router;

import lombok.Getter;

/** A precondition server only accepts messages if they match a precondition
 * set by a given function.
 * @param <M>   The type of the message.
 * @param <T>   The type of the message type.
 * @param <S>   The type of the implementing server.
 * Created by mwei on 11/27/16.
 */
public abstract class AbstractPreconditionServer<M extends IRoutableMsg<T>, T,
        S extends AbstractPreconditionServer<M, T, S>>
        extends AbstractServer<M, T> implements IPreconditionServer<M, T, S> {

    /** The precondition function, which must return true in order for the
     * message to be accepted by this server.
     */
    @Getter
    private final PreconditionFunction<M, T, S> preconditionFunction;

    /** The precondition message handler, which only accepts messages
     * which pass the precondition function.
     * @return  A precondition message handler.
     */
    public abstract PreconditionServerMsgHandler<M, T, S>
        getPreconditionMsgHandler();

    /** {@inheritDoc} */
    @Override
    public final ServerMsgHandler<M, T> getMsgHandler() {
        return getPreconditionMsgHandler();
    }

    /** Generate a new precondition server, using the given router
     * and precondition function.
     * @param router                The router this server is attached to.
     * @param precondition          A precondition function, which returns true
     *                              on messages which are to be accepted by
     *                              this server.
     */
    public AbstractPreconditionServer(final IServerRouter<M, T> router,
        final PreconditionFunction<M, T, S> precondition) {
        super(router);
        this.preconditionFunction = precondition;
    }


}
