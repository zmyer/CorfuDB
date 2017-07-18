package org.corfudb.test;

import com.google.gson.reflect.TypeToken;

import java.util.Properties;

import org.apache.http.cookie.SM;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.jupiter.engine.descriptor.ClassExtensionContext;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

/**
 * Created by mwei on 7/14/17.
 */
public class CorfuTestExtension implements
        TestInstancePostProcessor, BeforeAllCallback, BeforeTestExecutionCallback,
        BeforeEachCallback, ExecutionCondition,
        AfterTestExecutionCallback, AfterAllCallback, ParameterResolver {

    public static boolean isBuild() {
        return System.getProperties().containsKey("test");
    }

    public static void buildPrint(String toPrint) {
        if (isBuild()) {
            System.out.print(toPrint);
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {

    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) throws Exception {
        if (!extensionContext.getExecutionException().isPresent()) {
            buildPrint(Ansi.ansi().fgGreen().a(" ✔").reset() + "\n");
        } else {
            buildPrint(Ansi.ansi().fgRed().a(" ✘").reset() + "\n");
        }

        extensionContext.getStore(ExtensionContext.Namespace.create(CorfuTestExtension.class))
                .remove(CorfuTestInstance.class);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        if (isBuild()) {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            lc.reset(); // Disable loading from XML configuration
        }
        buildPrint(Ansi.ansi().fgMagenta()
                .a(extensionContext.getDisplayName()).reset() + "\n");
    }

    @Override
    public void beforeTestExecution(ExtensionContext extensionContext) throws Exception {
        // Set the correct logging level
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        LoggingLevel level = extensionContext.getTestMethod().get()
                .getAnnotation(LoggingLevel.class);
        if (level != null) {
            root.setLevel(level.level().level);
        } else {
            root.setLevel(Level.INFO);
        }
        // Generate a new CorfuTestInstance and put it in the store
        CorfuTestInstance ti = new CorfuTestInstance(extensionContext);
        extensionContext.getStore(ExtensionContext.Namespace.create(CorfuTestExtension.class))
                .put(CorfuTestInstance.class, ti);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.getParameter().getType().isAssignableFrom(CorfuRuntime.class)) {
            return true;
        } else if (parameterContext.getParameter().getType().isAssignableFrom(SMRMap.class)) {
            return true;
        }
        return false;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.getParameter().getType().isAssignableFrom(CorfuRuntime.class)) {
            return extensionContext.getStore(ExtensionContext.Namespace
                    .create(CorfuTestExtension.class)).get(CorfuTestInstance.class,
                    CorfuTestInstance.class).getRuntimeAsParameter(parameterContext);
        } else if (parameterContext.getParameter().getType().isAssignableFrom(SMRMap.class)) {
            return extensionContext.getStore(ExtensionContext.Namespace
                    .create(CorfuTestExtension.class)).get(CorfuTestInstance.class,
                    CorfuTestInstance.class).getRuntimeAsParameter(parameterContext)
                    .getObjectsView().build()
                        .setType(SMRMap.class)
                        .setStreamName("test-" + parameterContext.getIndex())
                        .open();
        }
        return null;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
    }

    @Override
    public void postProcessTestInstance(Object o,
                                        ExtensionContext extensionContext) throws Exception {
        // Reflective hack to get name
        //buildPrint(extensionContext
        //        .getTestDescriptor().getChildren().iterator().next().getDisplayName());
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        if (extensionContext.getTestMethod().isPresent()
                && extensionContext.getParent().isPresent()
                && !extensionContext.getParent().get().getTestMethod().isPresent()) {
            buildPrint(extensionContext.getDisplayName());
        } else if (
                extensionContext.getTestMethod().isPresent()
                        && extensionContext.getParent().isPresent()
                        && extensionContext.getParent().get().getTestMethod().isPresent()
                ) {
            // Print the name of the test
            buildPrint(Ansi.ansi().fgBlue().a("\n").a(extensionContext.getDisplayName()).reset()
                    .toString());
        }
        return ConditionEvaluationResult.enabled("");
    }
}
