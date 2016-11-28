package org.corfudb.exceptions;

/** Thrown when a client attempts to send a message but the endpoint
 * is in a disconnected state.
 * Created by mwei on 11/25/16.
 */
public class DisconnectedException extends NetworkException {

    /** Create a new disconnected exception.
     * @param endpoint  The endpoint which was disconnected.
     */
    public DisconnectedException(final String endpoint) {
        super("Attempted to send a message but the endpoint("
                + endpoint + ") is disconnected!", endpoint);
    }
}
