package org.corfudb.router;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.exceptions.ErrorResponseException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** An abstract client router. This router defines basic functionality for
 * handling and responding to requests.
 * @param <M> The message type, which must be routable and respondable.
 * @param <T> The type of the message type, which must be respondable.
 * Created by mwei on 12/6/16.
 */
@Slf4j
public abstract class AbstractClientRouter<M extends IRoutableMsg<T>
        & IRespondableMsg, T extends IRespondableMsgType<M>>
        implements IRequestClientRouter<M, T>  {


    /** A map of client class to clients. */
    private final Map<Class<? extends IClient<M, T>>, IClient<M, T>> clientMap =
            new ConcurrentHashMap<>();

    /** A map of message types to clients. */
    private final Map<T, IClient<M, T>> handlerMap =
            new ConcurrentHashMap<>();

    /** A map of completions, for request-response messages. */
    private final Map<Long, CompletableFuture<M>> completionMap =
            new ConcurrentHashMap<>();

    /** A counter for requests sent down the currently
     * registered channel context. */
    private final AtomicLong requestCounter = new AtomicLong(0);

    /** A scheduled executor service for timing out
     * requests.
     */
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            Executors.newScheduledThreadPool(
                    1,
                    new NamedThreadFactory("timeout"));


    /** The default amount of time to wait before timing out requests. */
    @Getter
    private final Duration defaultTimeout;

    /** Create a new client router, with the given timeout.
     * @param timeout    The default time to wait before timing out
     *                          requests.
     */
    public AbstractClientRouter(final Duration timeout) {
        this.defaultTimeout = timeout;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public <C extends IClient<M, T>> C getClient(final Class<C> clientType) {
        return (C) clientMap.get(clientType);
    }


    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public IClientRouter<M, T> registerClient(final IClient<M, T> client) {
        client.getHandledTypes().forEach(x -> handlerMap.put(x, client));
        clientMap.put((Class<? extends IClient<M, T>>)
                client.getClass(), client);
        return this;
    }


    /** {@inheritDoc} */
    @Override
    public IRequestClientRouter<M, T> registerRequestClient(
            final Function<IRequestClientRouter<M, T>,
            IClient<M, T>> clientSupplier) {
        IRequestClientRouter.super.registerRequestClient(clientSupplier);
        return this;
    }

    /**
     * Send a message and get a response as a completable future,
     * timing out if the response isn't received in the given amount of time.
     *
     * @param outMsg  The message to send.
     * @param timeout The maximum amount of time to wait for a response
     *                before timing out with a TimeoutException.
     * @return A completable future which will be completed with
     * the response, or completed exceptionally if there
     * was a communication error.
     */
    @Override
    public CompletableFuture<M> sendMessageAndGetResponse(final M outMsg,
                                                 final Duration timeout) {
        final CompletableFuture<M> future = new CompletableFuture<>();
        final long requestID = requestCounter.getAndIncrement();
        outMsg.setRequestID(requestID);
        completionMap.put(requestID, future);
        try {
            sendMessage(outMsg);
        } catch (Exception e) {
            // If an error occurs during the send of the request
            // remove it from the completion map and immediately
            // return an exceptionally completed future.
            future.completeExceptionally(e);
            completionMap.remove(requestID);
            return future;
        }
        return within(future, timeout, requestID);
    }

    /** Cancel all pending requests exceptionally. Typically used when
     * the endpoint unexpectedly disconnects.
     * @param e The exception to cancel all pending requests with.
     */
    protected void cancelAllPendingRequestsExceptionally(final Exception e) {
        completionMap.values().parallelStream()
                .forEach(x -> x.completeExceptionally(e));
        completionMap.clear();
    }

    /** Handle a message from the server.
     *
     * @param message   The incoming message
     * @param channel   The channel from where the message arrived
     */
    protected void handleServerMessage(final M message,
                                       final IChannel<M> channel) {
        if (message.getMsgType().isResponse()) {
            final CompletableFuture<M> future = completionMap.get(
                    message.getRequestID());
            if (future != null) {
                completionMap.remove(message.getRequestID());
                if (message.getMsgType().isError()) {
                    if (message.getMsgType().getExceptionGenerator() != null) {
                        future.completeExceptionally(message.getMsgType()
                                .getExceptionGenerator().apply(message));
                    } else {
                        future.completeExceptionally(
                                new ErrorResponseException(message));
                    }
                } else {
                    future.complete(message);
                }
            } else {
                log.error("Received a response for a message we don't "
                        + "have a completion for! "
                        + "Request ID={}", message.getRequestID());
            }
        } else {
            final IClient<M, T> client = handlerMap.get(message.getMsgType());
            if (client != null) {
                client.handleMessage(message, channel);
            }
        }
    }

    /**
     * Generates a completable future which times out. Removes the request from
     * the completion table on timeout.
     * inspired by NoBlogDefFound:
     * http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param duration  The duration to timeout after.
     * @param requestID The ID of the request.
     * @param <T>       Ignored, since the future will always timeout.
     * @return A completable future that will time out.
     */
    private <T> CompletableFuture<T> timeoutFuture(final Duration duration,
                                                   final long requestID) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        TIMEOUT_SCHEDULER.schedule(() -> {
            final TimeoutException ex = new TimeoutException("Timeout after "
                    + duration.toMillis() + " ms");
            completionMap.remove(requestID);
            return promise.completeExceptionally(ex);
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    /**
     * Takes a completable future, and ensures that it completes within a
     * certain duration. If it does not, it is cancelled and completes
     * exceptionally with TimeoutException, and removes the request
     * from the completion table.
     * inspired by NoBlogDefFound:
     * http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     * @param future   The completable future that must be completed within
     *                 duration.
     * @param duration The duration the future must be completed in.
     * @param requestID The ID of the request.
     * @param <T>      The return type of the future.
     * @return A completable future which completes with the original value
     * if completed within duration, otherwise completes exceptionally with
     * TimeoutException.
     */
    private <T> CompletableFuture<T> within(final CompletableFuture<T> future,
                           final Duration duration, final long requestID) {
        final CompletableFuture<T> timeout = timeoutFuture(duration, requestID);
        return future.applyToEither(timeout, Function.identity());
    }

}
