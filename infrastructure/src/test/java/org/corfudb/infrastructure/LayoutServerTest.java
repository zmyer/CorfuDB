package org.corfudb.infrastructure;


import lombok.extern.slf4j.Slf4j;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;


import java.util.UUID;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
@Slf4j
public class LayoutServerTest extends AbstractServerTest<LayoutServer> {

    public LayoutServerTest() {
        super(LayoutServer::new);
    }

    @Override
    public ServerContext getServerContext()
    {
        return new ServerContextBuilder()
                    .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                    .setSingle(false)
                    .build();
    }

    static final long LOW_RANK = 10L;
    static final long HIGH_RANK = 100L;

    /**
     * Verifies that a server that is not yet bootstrap does not respond with
     * a layout.
     */
    @Test
    public void nonBootstrappedServerNoLayout() {
        requestLayout(0);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_NOBOOTSTRAP_ERROR);
    }

    /**
     * Verifies that a server responds with a layout that the server was bootstrapped with.
     * There are no layout changes between bootstrap and layout request.
     */
    @Test
    public void bootstrapServerInstallsNewLayout() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);
        requestLayout(layout.getEpoch());
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout()).isEqualTo(layout);
    }

    /**
     * Verifies that a server cannot be bootstrapped multiple times.
     */
    @Test
    public void cannotBootstrapServerTwice() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);
        bootstrapServer(layout);
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP_ERROR);
    }


    /**
     * Verifies that once a prepare with a rank has been accepted,
     * any subsequent prepares with lower ranks are rejected.
     * Note: This is in the scope of same epoch.
     */
    @Test
    public void prepareRejectsLowerRanks() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        bootstrapServer(layout);

        sendPrepare(epoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        sendPrepare(epoch, LOW_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT_ERROR);
    }

    /**
     * Verifies that once a prepare with a rank has been accepted,
     * any propose with a lower rank is rejected.
     * Note: This is in the scope of same epoch.
     */
    @Test
    public void proposeRejectsLowerRanks() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        bootstrapServer(layout);
        sendPrepare(epoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        sendPropose(epoch, LOW_RANK, layout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR);
    }

    /**
     * Verifies that once a proposal has been accepted, the same proposal is not accepted again.
     * Note: This is in the scope of same epoch.
     */
    @Test
    public void proposeRejectsAlreadyProposed() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        bootstrapServer(layout);
        sendPrepare(epoch, LOW_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        sendPropose(epoch, LOW_RANK, layout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);
        sendPropose(epoch, LOW_RANK, layout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR);
    }

    /**
     * Verifies all phases set epoch, prepare, propose, commit.
     * Note: this is in the scope of a single epoch.
     */
    @Test
    public void commitReturnsAck() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        // set epoch on servers
        setEpoch(newEpoch);

        sendPrepare(newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);

        sendPropose(newEpoch, HIGH_RANK, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);
    }

    /**
     * Verifies that once set the epoch cannot regress.
     * Note: it does not verify that epoch is a dense monotonically increasing integer
     * sequence.
     */
    @Test
    public void checkServerEpochDoesNotRegress() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();

        bootstrapServer(layout);

        setEpoch(2);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);

        requestLayout(epoch);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        // NOTE: with the new router, the epoch in the response has no meaning.
        //assertThat(getLastMessage().getEpoch()).isEqualTo(2);

        setEpoch(1);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.WRONG_EPOCH_ERROR);

    }

    /**
     * Verifies that a layout is persisted across server reboots.
     *
     * @throws Exception
     */
    @Test
    public void checkLayoutPersisted() throws Exception {
        //serviceDirectory from which all instances of corfu server are to be booted.
        LayoutServer s1 = getServer();

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);

        final long OLD_EPOCH = 0;
        final long NEW_EPOCH = 100;

        newLayout.setEpoch(NEW_EPOCH);
        setEpoch(NEW_EPOCH);

        // Start the process of electing a new layout. But that layout will not take effect
        // till it is committed.
        sendPrepare(NEW_EPOCH, 1);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);

        sendPropose(NEW_EPOCH, 1, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);
        LayoutServerAssertions.assertThat(s1).isInEpoch(NEW_EPOCH);

        LayoutServerAssertions.assertThat(s1).isPhase1Rank(new Rank(1L, AbstractServerTest.TEST_CLIENT_ID));
        LayoutServerAssertions.assertThat(s1).isPhase2Rank(new Rank(1L, AbstractServerTest.TEST_CLIENT_ID));

        s1.stop();

        //TODO: refactor to use only one server.

        //assertThat(s2).isInEpoch(newEpoch);  // SLF: TODO: rebase conflict: new is 0, old was 100
        //assertThat(s2).isPhase1Rank(new Rank(1L, AbstractServerTest.testClientId));
        //assertThat(s2).isPhase2Rank(new Rank(1L, AbstractServerTest.testClientId))

        // request layout using the old epoch.
        requestLayout(OLD_EPOCH);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch()).isEqualTo(0);

        // request layout using the new epoch.
        requestLayout(NEW_EPOCH);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch()).isEqualTo(0);
    }

    /**
     * The test verifies that the data in accepted phase1 and phase2 messages
     * is persisted to disk and survives layout server restarts.
     *
     * @throws Exception
     */
    @Test
    public void checkPaxosPhasesPersisted() throws Exception {
        LayoutServer s1 = getServer();

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);

        // validate phase 1
        sendPrepare(newEpoch, 1);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        LayoutServerAssertions.assertThat(s1).isPhase1Rank(new Rank(1L, AbstractServerTest.TEST_CLIENT_ID));
       //shutdown this instance of server
        s1.stop();
        //bring up a new instance of server with the previously persisted data
        LayoutServer s2 = getServer();

        LayoutServerAssertions.assertThat(s2).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s2).isPhase1Rank(new Rank(1L, AbstractServerTest.TEST_CLIENT_ID));

        // validate phase2 data persistence
        sendPropose(newEpoch, 1, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);
        //shutdown this instance of server
        s2.stop();

        //bring up a new instance of server with the previously persisted data
        LayoutServer s3 = getServer();

        LayoutServerAssertions.assertThat(s3).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s3).isPhase1Rank(new Rank(1L,
                AbstractServerTest.TEST_CLIENT_ID));
        LayoutServerAssertions.assertThat(s3).isPhase2Rank(new Rank(1L, AbstractServerTest.TEST_CLIENT_ID));
        LayoutServerAssertions.assertThat(s3).isProposedLayout(newLayout);

    }

    /**
     * Validates that the layout server accept or rejects incoming phase1 messages based on
     * the last persisted phase1 rank.
     *
     * @throws Exception
     */
    @Test
    public void checkMessagesValidatedAgainstPhase1PersistedData() throws Exception {
        LayoutServer s1 = getServer();
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);
        // validate phase 1
        sendPrepare(newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        LayoutServerAssertions.assertThat(s1).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.TEST_CLIENT_ID));

        s1.stop();
        // reboot
        LayoutServer s2 = getServer();
        LayoutServerAssertions.assertThat(s2).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.TEST_CLIENT_ID));

        //new LAYOUT_PREPARE message with a lower phase1 rank should be rejected
        sendPrepare(newEpoch, HIGH_RANK - 1);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT_ERROR);


        //new LAYOUT_PREPARE message with a higher phase1 rank should be accepted
        sendPrepare(newEpoch, HIGH_RANK + 1);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
    }

    /**
     * Validates that the layout server accept or rejects incoming phase2 messages based on
     * the last persisted phase1 and phase2 data.
     * If persisted phase1 rank does not match the LAYOUT_PROPOSE message then the server did not
     * take part in the prepare phase. It should reject this message.
     * If the persisted phase2 rank is the same as incoming message, it will be rejected as it is a
     * duplicate message.
     *
     * @throws Exception
     */
    @Test
    public void checkMessagesValidatedAgainstPhase2PersistedData() throws Exception {
        LayoutServer s1 = getServer();
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s1).isInEpoch(newEpoch);

        // validate phase 1
        sendPrepare(newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        LayoutServerAssertions.assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.TEST_CLIENT_ID));

        s1.stop();

        LayoutServer s2 = getServer();
        LayoutServerAssertions.assertThat(s2).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.TEST_CLIENT_ID));

        //new LAYOUT_PROPOSE message with a lower phase2 rank should be rejected
        sendPropose(newEpoch,  HIGH_RANK - 1, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR);


        //new LAYOUT_PROPOSE message with a rank that does not match LAYOUT_PREPARE should be rejected
        sendPropose(newEpoch, HIGH_RANK + 1, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR);

        //new LAYOUT_PROPOSE message with same rank as phase1 should be accepted
        sendPropose(newEpoch, HIGH_RANK, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);
        LayoutServerAssertions.assertThat(s2).isProposedLayout(newLayout);

        s2.stop();
        // data should survive the reboot.
        LayoutServer s3 = getServer();
        LayoutServerAssertions.assertThat(s3).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s3).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.TEST_CLIENT_ID));
        LayoutServerAssertions.assertThat(s3).isProposedLayout(newLayout);
    }

    /**
     * Validates that the layout server accept or rejects incoming phase1 and phase2 messages from multiple
     * clients based on current state {Phease1Rank [rank, clientID], Phase2Rank [rank, clientID] }
     * If LayoutServer has accepted a phase1 message from a client , it can only accept a higher ranked phase1 message
     * from another client.
     * A phase2 message can only be accepted if the last accepted phase1 message is from the same client and has the
     * same rank.
     *
     * @throws Exception
     */
    @Test
    public void checkPhase1AndPhase2MessagesFromMultipleClients() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LayoutServer s1 = getServer();
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapServer(layout);

        long newEpoch = layout.getEpoch() + 1;
        Layout newLayout = TestLayoutBuilder.single(SERVERS.PORT_0);
        newLayout.setEpoch(newEpoch);

        setEpoch(newEpoch);

        /* validate phase 1 */
        sendPrepare(newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);
        LayoutServerAssertions.assertThat(s1).isPhase1Rank(new Rank(HIGH_RANK, AbstractServerTest.TEST_CLIENT_ID));

        // message from a different client with same rank should be rejected or accepted based on
        // whether the uuid is greater of smaller.
        sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT_ERROR);

        sendPrepare(UUID.nameUUIDFromBytes("TEST_CLIENT_OTHER".getBytes()), newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_PREPARE_REJECT_ERROR);

        // message from a different client but with a higher rank gets accepted
        sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK + 1);
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);


        // testing behaviour after server restart
        resetTest();

        LayoutServer s2 = getServer();
        LayoutServerAssertions.assertThat(s2).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s2).isPhase1Rank(
                new Rank(HIGH_RANK + 1,
                UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        //duplicate message to be rejected
        sendPrepare(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()),
                newEpoch, HIGH_RANK + 1);
        assertThat(getLastMessage().getMsgType()).isEqualTo(
                CorfuMsgType.LAYOUT_PREPARE_REJECT_ERROR);

        /* validate phase 2 */

        //phase2 message from a different client than the one whose phase1 was last accepted is rejected
        sendPropose(newEpoch, HIGH_RANK + 1, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PROPOSE_REJECT_ERROR);

        // phase2 from same client with same rank as in phase1 gets accepted
        sendPropose(UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes()), newEpoch, HIGH_RANK + 1, newLayout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);

        LayoutServerAssertions.assertThat(s2).isInEpoch(newEpoch);
        LayoutServerAssertions.assertThat(s2).isPhase1Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        LayoutServerAssertions.assertThat(s2).isPhase2Rank(new Rank(HIGH_RANK + 1, UUID.nameUUIDFromBytes("OTHER_CLIENT".getBytes())));
        LayoutServerAssertions.assertThat(s2).isProposedLayout(newLayout);

        s2.stop();
    }

//    @Test
//    public void testReboot() throws Exception {
//        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
//        LayoutServer s1 = getDefaultServer(serviceDir);
//        setServer(s1);
//
//        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
//        final long NEW_EPOCH = 99L;
//        layout.setEpoch(NEW_EPOCH);
//        bootstrapServer(layout);
//
//        // Reboot, then check that our epoch 100 layout is still there.
//        //s1.reboot();
//
//        requestLayout(NEW_EPOCH);
//        Assertions.assertThat(getLastMessage().getMsgType())
//                .isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
//        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout().getEpoch()).isEqualTo(NEW_EPOCH);
//        s1.shutdown();
//
//        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
//            LayoutServer s2 = getDefaultServer(serviceDir);
//            setServer(s2);
//            commitReturnsAck(s2, i, NEW_EPOCH + 1);
//            s2.shutdown();
//        }
//    }

    private void commitReturnsAck(LayoutServer s1, Integer reboot, long baseEpoch) {

        long newEpoch = baseEpoch + reboot;
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.SEAL_EPOCH, newEpoch));

        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        layout.setEpoch(newEpoch);

        sendPrepare(newEpoch, HIGH_RANK);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_PREPARE_ACK_RESPONSE);

        sendPropose(newEpoch, HIGH_RANK, layout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);

        sendCommitted(newEpoch, layout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);

        sendCommitted(newEpoch, layout);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK_RESPONSE);

        requestLayout(newEpoch);
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.LAYOUT_RESPONSE);
        Assertions.assertThat(((LayoutMsg) getLastMessage()).getLayout()).isEqualTo(layout);

    }

    private void bootstrapServer(Layout l) {
        sendMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l)));
    }

    private void requestLayout(long epoch) {
        sendMessage(CorfuMsgType.LAYOUT_REQUEST.payloadMsg(epoch));
    }

    private void setEpoch(long epoch) {
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.SEAL_EPOCH, epoch));
    }

    private void sendPrepare(long epoch, long rank) {
        sendMessage(CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)));
    }

    private void sendPropose(long epoch, long rank, Layout layout) {
        sendMessage(CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch, rank, layout)));
    }

    private void sendCommitted(long epoch, Layout layout) {
        sendMessage(CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }

    private void sendPrepare(UUID clientId, long epoch, long rank) {
        sendMessage(clientId, CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)));
    }

    private void sendPropose(UUID clientId, long epoch, long rank, Layout layout) {
        sendMessage(clientId, CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch, rank, layout)));
    }

    private void sendCommitted(UUID clientId, long epoch, Layout layout) {
        sendMessage(clientId, CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }

}
