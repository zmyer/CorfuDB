package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.util.CFUtils;
import org.junit.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 7/27/16.
 */
public class BaseClientTest
        extends AbstractClientTest<BaseClient, BaseServer> {

    BaseClientTest() {
        super(BaseClient::new, BaseServer::new);
    }

    @Test
    public void canPing() {
        assertThat(getClient().pingSync())
                .isTrue();
    }

    @Test
    public void canGetVersionInfo() {
        CFUtils.getUninterruptibly(client.getVersionInfo());
    }
}
