package org.corfudb.exceptions;

import lombok.Getter;
import org.corfudb.router.IRespondableMsgType;
import org.corfudb.router.IRoutableMsg;

/** An exception which is thrown when a server throws an error in response
 * to a request, and no translation is set for that exception.
 * Created by mwei on 11/28/16.
 */
public class ErrorResponseException extends RuntimeException {

    /** The error message that was sent. */
    @Getter
    private final IRoutableMsg errorMessage;

    /** Get a new error response, with the given message.
     * @param msg   The message which was thrown by the server.
     */
    public ErrorResponseException(final IRoutableMsg msg) {
        super("Server returned " + msg.getMsgType().toString());
        this.errorMessage = msg;
    }

    /** Get the type of the error which was thrown.
     * @return  The type of the message.
     */
    public IRespondableMsgType getErrorType() {
        return (IRespondableMsgType) errorMessage.getMsgType();
    }

    /** Get the error message type, casting to the type given.
     * @param type  The type to cast to.
     * @param <T>   The type of the message type.
     * @return      The message type, casted to the given type.
     */
    @SuppressWarnings("unchecked")
    public <T extends IRespondableMsgType> T getErrorType(final Class<T> type) {
        return (T) errorMessage.getMsgType();
    }
}
