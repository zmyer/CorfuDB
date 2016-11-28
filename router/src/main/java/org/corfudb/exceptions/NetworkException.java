package org.corfudb.exceptions;

import lombok.Getter;

/** Represents a connection caused by a network issue to
 * a remote endpoint.
 * Created by mwei on 11/25/16.
 */
public class NetworkException extends RuntimeException {

    /** The endpoint which caused the network exception. */
    @Getter
    private final String endpoint;

    /** Generate a new NetworkException from an endpoint string.
     * @param ep  The endpoint which generated the exception.
     */
    public NetworkException(final String ep) {
        super("General Network failure connecting to endpoint " + ep);
        this.endpoint = ep;
    }

    /** Generate a new NetworkException from an endpoint string plus exception.
     * @param ep        The endpoint which generated the exception.
     * @param cause     The reason for the exception.
     */
    public NetworkException(final String ep, final Throwable cause) {
        super("General Network failure connecting to endpoint " + ep, cause);
        this.endpoint = ep;
    }

    /** Generate a new NetworkException from an endpoint string plus exception.
     * @param message   The message for the exception.
     * @param ep        The endpoint which generated the exception.
     */
    public NetworkException(final String message, final String ep) {
        super(message);
        this.endpoint = ep;
    }

    /** Generate a new NetworkException from an endpoint string plus exception.
     * @param message   The message for the exception.
     * @param ep        The endpoint which generated the exception.
     * @param cause     The reason for the exception.
     */
    public NetworkException(final String message,
                            final String ep,
                            final Throwable cause) {
        super(message, cause);
        this.endpoint = ep;
    }
}
