package org.corfudb.runtime.view.stream;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.test.InterfaceParameter;
import org.corfudb.test.InterfaceTest;
import org.corfudb.test.Iterations;

public interface StreamViewContract<S extends IStreamView, E> extends TestableStreamView<S, E> {

    @InterfaceTest
    default void emptyStreamNextReturnsNull(S s) {
        assertThat(s.next())
                .isNull();
    }

    @InterfaceTest
    default void emptyStreamNextUpToReturnsNull(S s) {
        assertThat(s.nextUpTo(Address.MAX))
                .isNull();
        assertThat(s.nextUpTo(0))
                .isNull();
    }

    @InterfaceTest
    default void canReadOwnAppend(CorfuRuntime r, S s, E e) {
        s.append(e);
        assertThat(s.hasNext())
                .isTrue();
        assertThat(s.next().getPayload(r))
                .isEqualTo(e);
        assertThat(s.next())
                .isNull();
    }

    @InterfaceTest
    default void canReadUpToOwnAppend(CorfuRuntime r, S s, E e) {
        s.append(e);
        assertThat(s.hasNext())
                .isTrue();
        assertThat(s.nextUpTo(Address.MAX).getPayload(r))
                .isEqualTo(e);
        assertThat(s.nextUpTo(Address.MAX))
                .isNull();
    }

    @InterfaceTest
    default void canReadOwnAppendMultiple(CorfuRuntime r, S s, @Iterations int iterations) {
        for (int i = 0; i < iterations; i++) {
            E e = createEntry();
            s.append(e);
            assertThat(s.hasNext())
                    .isTrue();
            assertThat(s.next().getPayload(r))
                    .isEqualTo(e);
        }
        assertThat(s.next())
                .isNull();
    }

    @InterfaceTest
    default void streamUpdatesAreIndependent(CorfuRuntime r, S s1, S s2, E e1, E e2) {
        s1.append(e1);

        assertThat(s1.hasNext())
                .isTrue();
        assertThat(s2.hasNext())
                .isFalse();

        s2.append(e2);

        assertThat(s2.hasNext())
                .isTrue();
        assertThat(s1.next().getPayload(r))
                .isEqualTo(e1);
        assertThat(s1.next())
                .isNull();
        assertThat(s2.next().getPayload(r))
                .isEqualTo(e2);
        assertThat(s2.next())
                .isNull();
    }

    @InterfaceTest
    default void sameStreamHasSameUpdates(CorfuRuntime r,
                                          S s1, @InterfaceParameter(index=0) S s1New,
                                          E e1, E e2) {
        s1.append(e1);

        assertThat(s1New.hasNext())
                .isTrue();
        assertThat(s1New.next().getPayload(r))
                .isEqualTo(e1);

        s1New.append(e2);

        assertThat(s1.next().getPayload(r))
                .isEqualTo(e1);
        assertThat(s1.next().getPayload(r))
                .isEqualTo(e2);
        assertThat(s1New.hasNext())
                .isTrue();
        assertThat(s1New.next().getPayload(r))
                .isEqualTo(e2);
    }
}
