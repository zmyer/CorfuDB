package org.corfudb.router;

/** Represents a channel, which is used to send messages.
 * @param <M> The type of the messages sent on this channel.
 * Created by mwei on 11/29/16.
 */
public interface IChannel<M> {
    /** Send a message on this channel.
     * @param message   The message to send.
     */
    void sendMessage(M message);
}
