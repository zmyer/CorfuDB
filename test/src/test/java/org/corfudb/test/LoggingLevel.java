package org.corfudb.test;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by mwei on 7/14/17.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LoggingLevel {

    @AllArgsConstructor
    enum Level {
        OFF(ch.qos.logback.classic.Level.OFF),
        ERROR(ch.qos.logback.classic.Level.ERROR),
        WARN(ch.qos.logback.classic.Level.WARN),
        DEBUG(ch.qos.logback.classic.Level.DEBUG),
        TRACE(ch.qos.logback.classic.Level.TRACE),
        ALL(ch.qos.logback.classic.Level.ALL);

        @Getter
        final ch.qos.logback.classic.Level level;
    }

    Level level();
}
