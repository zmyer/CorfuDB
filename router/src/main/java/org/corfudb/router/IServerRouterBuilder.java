package org.corfudb.router;

/** An interface for builders of server routers.
 * @param <M> The type of messages the server router should handle.
 * @param <T> The type of the message types the server router should handle.
 * Created by mwei on 12/6/16.
 */
public interface IServerRouterBuilder<M extends IRoutableMsg<T>, T> {
    /** Obtain a new instance of the server router from the given parameters.
     * @return  A new server router.
     */
    IServerRouter<M, T> build();
}
