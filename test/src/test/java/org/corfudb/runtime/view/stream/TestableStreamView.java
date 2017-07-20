package org.corfudb.runtime.view.stream;

import org.corfudb.test.TestableInterface;

@TestableInterface
public interface TestableStreamView<T extends IStreamView, E> {

    T createStreamView();
    T createStreamView(int index, boolean existing);

    E createEntry();
}
