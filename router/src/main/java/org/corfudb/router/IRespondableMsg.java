package org.corfudb.router;

/** Represents a message which can be responded to, based on a request ID.
 * Created by mwei on 11/24/16.
 */
public interface IRespondableMsg {

    /** Get the request ID for this message.
     * @return The request ID for this message. */
    long getRequestID();

    /** Set the request ID for this message.
     * @param requestID The request ID for this message.
     */
    void setRequestID(long requestID);

    /** Copy fields from this request message to the response for this
     * message.
     * @param outMsg    The response message to copy fields to.
     */
    default void copyFieldsToResponse(IRespondableMsg outMsg) {
        outMsg.setRequestID(getRequestID());
    }
}
