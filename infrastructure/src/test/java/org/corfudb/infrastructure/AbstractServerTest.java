package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.router.IChannel;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.test.TestServerRouter;
import org.junit.Before;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

/** A server test enables unit testing of a single server.
 *
 * These tests should test the behavior of servers independent of clients
 * or other servers.
 *
 * Created by mwei on 12/12/15.
 */
public abstract class AbstractServerTest
        <S extends IServer<CorfuMsg, CorfuMsgType>>
        extends AbstractCorfuTest {

    /** The simulated client ID for this test. */
    public static final UUID TEST_CLIENT_ID = UUID
            .nameUUIDFromBytes("TEST_CLIENT".getBytes());

    /** A test server router which will be used to route messages
     * to the server. */
    private TestServerRouter<CorfuMsg, CorfuMsgType> router;

    /** The function used to generate new servers.
     */
    private final
        Function<IServerRouter<CorfuMsg, CorfuMsgType>, S> serverFactory;

    /** The current server for this test. */
    private IServer<CorfuMsg, CorfuMsgType> server;

    /** The simulated channel for this test. */
    private SimpleTestChannel testChannel;

    /** Constructor, using a function which generates servers.
     * @param serverFactory A function, which given the router, generates a
     *                      server.
     */
    public AbstractServerTest(
            Function<IServerRouter<CorfuMsg, CorfuMsgType>, S> serverFactory) {
        this.serverFactory = serverFactory;
    }

    /** Constructor, using a function with generates servers with a context.
     * @param serverFactory A function, which given the router and context,
     *                      generates a server.
     */
    public AbstractServerTest(BiFunction<IServerRouter<CorfuMsg, CorfuMsgType>,
            ServerContext, S> serverFactory) {
        this.serverFactory = r -> serverFactory.apply(r, getServerContext());
    }

    /** Get the server context, which defaults to a default context using port
     * 0. Override this function if your test requires a different context type.
     * @return  A new server context.
     */
    public ServerContext getServerContext() {
        return ServerContextBuilder.defaultContext(0);
    }

    /** Get the server for this test.
     * @return  The server for this test. */
    public S getServer() {
        return (S) server;
    }

    /** Reset this test. */
    @Before
    public void resetTest() {
        router = new TestServerRouter<>();
        testChannel = new SimpleTestChannel();
        server = serverFactory.apply(router);
        router.registerServer(server);
        router.start();
    }

    /** A simple test channel which records all messages the server
     * responds with.
     */
    public class SimpleTestChannel implements IChannel<CorfuMsg> {

        /** The list of messages the server has responded with. */
        List<CorfuMsg> responseMessages = new CopyOnWriteArrayList<>();
        /** A counter which will stamp all outgoing messages. */
        AtomicLong requestID = new AtomicLong();

        /** {@inheritDoc} */
        @Override
        public void sendMessage(CorfuMsg message) {
            responseMessages.add(message);
        }

        /** Read something on this channel.
         * @param message   The message to read.
         */
        public void channelRead(CorfuMsg message) {
            router.handleMessage(this, message);
        }
    }

    /** Get a list of messages the server has responded with. */
    public List<CorfuMsg> getResponseMessages() {
        return testChannel.responseMessages;
    }

    /** Get the last message the server responded with. */
    public CorfuMsg getLastMessage() {
        if (testChannel.responseMessages.size() == 0) return null;
        return testChannel.responseMessages.get(testChannel.responseMessages.size() - 1);
    }

    /** Get the last message as a specific type.
     * @param <T> The type of the message.
     * @param type The class representing the type of the message.
     */
    @SuppressWarnings("unchecked")
    public <T extends CorfuMsg> T getLastMessageAs(Class<T> type) {
        return (T) getLastMessage();
    }

    /** Get the last payload message cast as a specific type.
     *
     * @param type  The class representing the type of payload.
     * @param <T>   The type of payload.
     * @return      The payload.
     */
    @SuppressWarnings("unchecked")
    public <T> T getLastPayloadMessageAs(Class<T> type) {
        Assertions.assertThat(getLastMessage())
                .isInstanceOf(CorfuPayloadMsg.class);
        return ((CorfuPayloadMsg<T>)getLastMessage()).getPayload();
    }

    /** Send a message to the server.
     * @param message   The message to send. */
    public void sendMessage(CorfuMsg message) {
        sendMessage(TEST_CLIENT_ID, message);
    }

    /** Send a message to the server, using a specific client ID.
     * @param clientId  The client ID to use.
     * @param message   The message to send.
     */
    public void sendMessage(UUID clientId, CorfuMsg message) {
        message.setClientID(clientId);
        message.setRequestID(testChannel.requestID.getAndIncrement());
        testChannel.channelRead(message);
    }

}
