package org.corfudb.runtime.clients;

import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/21/15.
 */
public class LayoutClientTest extends AbstractClientTest<LayoutClient,
                                                            LayoutServer> {

    public LayoutClientTest() {
        super(LayoutClient::new, LayoutServer::new);
    }

    /** For these tests, we want a server which has not been
     * bootstrapped.
     * @return  A server context without bootstrap (non-single).
     */
    @Override
    public ServerContext getServerContext() {
        return new ServerContextBuilder()
                .setSingle(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .build();
    }

    @Test
    public void nonBootstrappedServerThrowsException() {
        assertThatThrownBy(() -> {
            getClient().getLayout().get();
        }).hasCauseInstanceOf(NoBootstrapException.class);
    }

    @Test
    public void bootstrapServerInstallsNewLayout()
            throws Exception {
        assertThat(getClient()
                .bootstrapLayout(TestLayoutBuilder.single(SERVERS.PORT_0))
                        .get())
                .isEqualTo(true);

        assertThat(getClient().getLayout().get().asJSONString())
                .isEqualTo(TestLayoutBuilder.single(SERVERS.PORT_0)
                        .asJSONString());
    }

    @Test
    public void cannotBootstrapServerTwice()
            throws Exception {
        assertThat(getClient().bootstrapLayout(TestLayoutBuilder
                .single(SERVERS.PORT_0)).get())
                .isEqualTo(true);
        assertThatThrownBy(() -> getClient()
                .bootstrapLayout(TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .hasCauseInstanceOf(AlreadyBootstrappedException.class);
    }

    @Test
    public void canGetNewLayoutInDifferentEpoch()
            throws Exception {
        Layout l = TestLayoutBuilder.single(SERVERS.PORT_0);
        final long NEW_EPOCH = 42L;
        l.setEpoch(NEW_EPOCH);
        assertThat(getClient().bootstrapLayout(l).get())
                .isEqualTo(true);

        assertThat(getClient().getLayout().get().getEpoch())
                .isEqualTo(NEW_EPOCH);
    }

    final long RANK_LOW = 5L;
    final long RANK_HIGH = 10L;
    @Test
    public void prepareRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        assertThat(getClient().bootstrapLayout(layout).get())
                .isEqualTo(true);
        long epoch = layout.getEpoch();
        assertThat(getClient().prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            getClient().prepare(epoch, RANK_LOW).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            getClient().prepare(epoch, 2L).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void proposeRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        assertThat(getClient().bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(getClient().prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            getClient().propose(epoch, RANK_LOW, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThat(getClient().propose(epoch, RANK_HIGH,
                TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .isEqualTo(true);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        assertThat(getClient().bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(getClient().prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);

        getClient().propose(epoch, RANK_HIGH, layout).get();

        assertThatThrownBy(() -> {
            getClient().propose(epoch, RANK_LOW, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            getClient().propose(epoch, RANK_HIGH, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void commitReturnsAck()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        assertThat(getClient().bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(getClient().prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);
        final long TEST_EPOCH = 777;
        layout.setEpoch(TEST_EPOCH);

        assertThat(getClient().committed(TEST_EPOCH, layout).get())
                .isEqualTo(true);
    }

}
