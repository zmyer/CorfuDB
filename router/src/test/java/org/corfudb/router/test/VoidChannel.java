package org.corfudb.router.test;

import org.corfudb.router.IChannel;
import org.corfudb.router.IRespondableMsg;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.router.IRoutableMsg;

/** A channel that discards messages sent to it.
 * Created by mwei on 12/19/16.
 */
public class VoidChannel<M extends IRoutableMsg<T> & IRespondableMsg,
        T extends IRespondableMsgType<M>> implements IChannel<M> {
    /**
     * Send a message on this channel.
     *
     * @param message The message to send.
     */
    @Override
    public void sendMessage(M message) {

    }
}
