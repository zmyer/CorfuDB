package org.corfudb.infrastructure;

import org.junit.Test;

/**
 * Created by mwei on 12/8/16.
 */
public class SequencerServerPersistenceTest
        extends AbstractServerTest<SequencerServer> {

    public SequencerServerPersistenceTest() {
        super(SequencerServer::new);
    }

    /**
     * Get a persistent server context.
     *
     * @return A new server context.
     */
    @Override
    public ServerContext getServerContext() {
        return new ServerContextBuilder()
                .setCheckpoint(1)
                .build();
    }

    @Test
    public void checkSequencerCheckpointingWorks()
            throws Exception {
        String serviceDir = getTempDir();

        /*
        SequencerServer s1 = new SequencerServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(0)
                .setCheckpoint(1)
                .build());

        this.router.reset();
        this.router.addServer(s1);
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQUEST,
                new TokenRequest(1L, Collections.singleton(CorfuRuntime.getStreamID("a")), false, false)));
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQUEST,
                new TokenRequest(1L, Collections.singleton(CorfuRuntime.getStreamID("a")), false, false)));
        assertThat(s1)
                .tokenIsAt(2);
        Thread.sleep(1400);
        s1.shutdown();

        SequencerServer s2 = new SequencerServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(-1)
                .setCheckpoint(1)
                .build());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2)
                .tokenIsAt(2);
                */
    }

}
