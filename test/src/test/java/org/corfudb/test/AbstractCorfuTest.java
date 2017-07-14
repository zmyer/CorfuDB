package org.corfudb.test;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;

import org.corfudb.runtime.CorfuRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestReporter;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 7/13/17.
 */
@CorfuTestClass
@Slf4j
@DisplayName("Base Corfu Test")
public class AbstractCorfuTest {

    @CorfuTest
    @DisplayName("Hello 1")
    @LoggingLevel(level=LoggingLevel.Level.ALL)
    void standardAssertions(CorfuRuntime r) {
        r.getSequencerView().nextToken(
                Collections.singleton(CorfuRuntime.getStreamID("a")), 1);
    }

    @CorfuTest
    @DisplayName("Hello 2")
    void standardException(CorfuRuntime r1, CorfuRuntime r2) {
        r1.getSequencerView().nextToken(
                Collections.singleton(CorfuRuntime.getStreamID("a")), 1);
        r2.getSequencerView().nextToken(
                Collections.singleton(CorfuRuntime.getStreamID("a")), 1);
    }


}
