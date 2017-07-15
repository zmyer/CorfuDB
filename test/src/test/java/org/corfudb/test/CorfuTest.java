package org.corfudb.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Created by mwei on 7/14/17.
 */
@ExtendWith(CorfuTestExtension.class)
@Test
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CorfuTest {
}
