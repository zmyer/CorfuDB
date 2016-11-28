package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class BaseServerTest extends AbstractServerTest<BaseServer> {

    public BaseServerTest() {
        super(BaseServer::new);
    }

    @Test
    public void testPing() {
        sendMessage(CorfuMsgType.PING.msg());
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.PONG_RESPONSE);
    }

    @Test
    public void shutdownServerDoesNotRespond() {
        Assertions.assertThat(getLastMessage())
                .isNull();
        sendMessage(CorfuMsgType.PING.msg());
        Assertions.assertThat(getLastMessage())
                .isNull();
    }
}
