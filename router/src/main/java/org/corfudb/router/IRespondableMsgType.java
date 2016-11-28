package org.corfudb.router;

import java.util.function.Function;

/** A message type which is respondable.
 * @param <M> The type of messages this type can be assigned to.
 * Created by mwei on 11/24/16.
 */
public interface IRespondableMsgType<M extends IRespondableMsg> {
    /** Whether or not this message type is a response to a request.
     * @return  True, if the message type is a response, false otherwise.
     */
    boolean isResponse();

    /** Whether or not this message type is an error response.
     * @return  True, if the message type is an error response, false otherwise.
     */
    boolean isError();

    /** If this message is an error, how to generate an exception for this
     * message to pass to the client.
     * @param <E>   The type of exception this generator creates.
     * @return      A generator for the exception, given the message, or
     *              null, if a default exception should be generated.
     *              If isError() returns false, this should be set to null.
     */
    <E extends Throwable> Function<M, E> getExceptionGenerator();
}
