package org.corfudb.router;

import com.google.common.reflect.TypeToken;
import java.time.Duration;

/** An interface that represents shared fields for a client router builder.
 * @param <M> The message type of the client router.
 * @param <T> The type of the message type of the client router.
 * Created by mwei on 12/6/16.
 */
public interface IClientRouterBuilder
        <M extends IRoutableMsg<T> & IRespondableMsg,
                T extends IRespondableMsgType<M>> {
    /** Set the default timeout for requests.
     * @param timeout   The duration that requests should timeout.
     * @return          This builder, to support chaining.
     */
    IClientRouterBuilder<M, T> setDefaultTimeout(Duration timeout);

    /** Set whether or not this client router should automatically reconnect.
     * @param automaticallyReconnect    True, if this router should reconnect
     *                                  automatically.
     * @return          This builder, to support chaining.
     */
    IClientRouterBuilder<M, T>
        setAutomaticallyReconnect(boolean automaticallyReconnect);

    /** Get the builder using the given type.
     * @param typeToken     The type token to view this builder as.
     * @param <R>           The type of the type token.
     * @return              This builder, cast as the type given.
     */
    default <R extends IClientRouterBuilder<M, T>> R
        getBuilderAs(TypeToken<R> typeToken) {
        return (R) this;
    }

    /** Build a client router using the given parameters.
     * @return              A new client router, built using the parameters
     *                      given.
     */
    IRequestClientRouter<M, T> build();
}
