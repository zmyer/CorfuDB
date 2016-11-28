package org.corfudb.router;

/** Represents a message which can be routed to multiple servers/clients.
 * @param <T> The type of the message type.
 * Created by mwei on 11/23/16.
 */
public interface IRoutableMsg<T> {

    /** Retrieve the message type for this message.
     * @return  The message type for this message.
     */
    T getMsgType();
}
