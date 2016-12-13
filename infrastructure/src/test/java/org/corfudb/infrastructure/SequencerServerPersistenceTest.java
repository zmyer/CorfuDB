package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;

import static org.corfudb.infrastructure.SequencerServerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

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
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .build();
    }

    @Test
    public void checkSequencerCheckpointingWorks()
            throws Exception {


        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQUEST,
                new TokenRequest(1L,
                        Collections.singleton(CorfuRuntime
                                                .getStreamID("a")),
                        false,
                        false)));

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQUEST,
                new TokenRequest(1L,
                        Collections.singleton(CorfuRuntime
                                                .getStreamID("a")),
                        false,
                        false)));

        final SequencerServer FIRST_SEQUENCER_INSTANCE = getServer();

        assertThat(FIRST_SEQUENCER_INSTANCE)
                .tokenIsAt(2);

        // TODO: Have a command to send to the sequencer to force
        // checkpointing.
        Thread.sleep(PARAMETERS.TIMEOUT_NORMAL.toMillis());

        resetTest();

        final SequencerServer SECOND_SEQUENCER_INSTANCE = getServer();

        assertThat(FIRST_SEQUENCER_INSTANCE)
                .isNotEqualTo(SECOND_SEQUENCER_INSTANCE);

        assertThat(SECOND_SEQUENCER_INSTANCE)
                .tokenIsAt(2);
    }

}
