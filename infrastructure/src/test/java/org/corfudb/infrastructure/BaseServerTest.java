package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the base server basic functionality.
 * Created by mwei on 12/14/15.
 */
public class BaseServerTest extends AbstractServerTest<BaseServer> {

    public BaseServerTest() {
        super(BaseServer::new);
    }

    /** Test that pinging the base server works.
     * The server should respond to a PING with a PONG.
     */
    @Test
    public void testPing() {
        sendMessage(CorfuMsgType.PING.msg());
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.PONG_RESPONSE);
    }

}
