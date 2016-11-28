package org.corfudb.infrastructure;

import lombok.Getter;
import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.router.AbstractServer;
import org.corfudb.router.IChannel;
import org.corfudb.router.IServer;
import org.corfudb.router.IServerRouter;
import org.corfudb.router.test.TestClientRouter;
import org.corfudb.router.test.TestServerRouter;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/12/15.
 */
public abstract class AbstractServerTest<S extends IServer<CorfuMsg,CorfuMsgType>> extends AbstractCorfuTest {

    public static final UUID testClientId = UUID.nameUUIDFromBytes("TEST_CLIENT".getBytes());

    private TestServerRouter<CorfuMsg, CorfuMsgType> router;
    private final Function<IServerRouter<CorfuMsg, CorfuMsgType>, S> serverFactory;
    private IServer<CorfuMsg, CorfuMsgType> server;
    private SimpleTestChannel testChannel;

    public AbstractServerTest(Function<IServerRouter<CorfuMsg, CorfuMsgType>, S> serverFactory) {
        this.serverFactory = serverFactory;
    }

    public AbstractServerTest(BiFunction<IServerRouter<CorfuMsg, CorfuMsgType>, ServerContext, S> serverFactory) {
        this.serverFactory = r -> serverFactory.apply(r, getServerContext());
    }


    public ServerContext getServerContext() {
        return ServerContextBuilder.defaultContext(0);
    }

    public S getServer() {
        return (S) server;
    }

    @Before
    public void resetTest() {
        router = new TestServerRouter<>();
        testChannel = new SimpleTestChannel();
        server = serverFactory.apply(router);
        router.registerServer(server);
        router.start();
    }

    public class SimpleTestChannel implements IChannel<CorfuMsg> {

        List<CorfuMsg> responseMessages = new CopyOnWriteArrayList<>();
        AtomicLong requestID = new AtomicLong();

        @Override
        public void sendMessage(CorfuMsg message) {
            responseMessages.add(message);
        }

        // Read something on this channel.
        public void channelRead(CorfuMsg message) {
            router.handleMessage(this, message);
        }
    }

    public List<CorfuMsg> getResponseMessages() {
        return testChannel.responseMessages;
    }

    public CorfuMsg getLastMessage() {
        if (testChannel.responseMessages.size() == 0) return null;
        return testChannel.responseMessages.get(testChannel.responseMessages.size() - 1);
    }

    @SuppressWarnings("unchecked")
    public <T extends CorfuMsg> T getLastMessageAs(Class<T> type) {
        return (T) getLastMessage();
    }

    @SuppressWarnings("unchecked")
    public <T> T getLastPayloadMessageAs(Class<T> type) {
        Assertions.assertThat(getLastMessage())
                .isInstanceOf(CorfuPayloadMsg.class);
        return ((CorfuPayloadMsg<T>)getLastMessage()).getPayload();
    }
    public void sendMessage(CorfuMsg message) {
        sendMessage(testClientId, message);
    }

    public void sendMessage(UUID clientId, CorfuMsg message) {
        message.setClientID(clientId);
        message.setRequestID(testChannel.requestID.getAndIncrement());
        testChannel.channelRead(message);
    }

}
