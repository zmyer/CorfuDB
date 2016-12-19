package org.corfudb.runtime.clients;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;

import java.util.Collections;

/**
 * Created by mwei on 12/18/16.
 */
public class TestRuntime extends CorfuRuntime {

    public class TestLayoutView extends LayoutView {

        public TestLayoutView(CorfuRuntime runtime) {
            super(runtime);
        }

        /**
         * Get the current layout.
         *
         * @return The current layout.
         */
        @Override
        public Layout getCurrentLayout() {
            return getLayout();
        }

        @Override
        public Layout getLayout() {
            return new Layout(Collections.emptyList(),
                              Collections.emptyList(),
                              Collections.emptyList(),
                              0L);
        }


    }
    /**
     * A view of the layout service in the Corfu server instance.
     */
    @Override
    public LayoutView getLayoutView() {
        return new TestLayoutView(this);
    }
}
