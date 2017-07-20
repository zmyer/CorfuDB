package org.corfudb.test;

import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.engine.descriptor.ClassExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
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
        AfterTestExecutionCallback, AfterAllCallback, ParameterResolver,
        TestTemplateInvocationContextProvider {

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
        final String methodName = extensionContext.getRoot().getStore(ExtensionContext.Namespace
                .create(CorfuTestExtension.class))
                .get("method", String.class);
        final String testName = extensionContext.getRoot().getStore(ExtensionContext.Namespace
                .create(CorfuTestExtension.class))
                .get("test", String.class);
        final String lastMethod = extensionContext.getRoot().getStore(ExtensionContext.Namespace
                .create(CorfuTestExtension.class))
                .get("lastmethod", String.class);
        if (testName != null) {
            if (lastMethod == null) {
                buildPrint(Ansi.ansi().fgYellow().a(methodName).a("\n").reset().toString());
                extensionContext.getRoot().getStore(ExtensionContext.Namespace
                        .create(CorfuTestExtension.class)).put("lastmethod", methodName);
            }
            buildPrint(Ansi.ansi().fgBlue().a(extensionContext.getDisplayName()).reset()
                    .toString());
        }
        else {
            buildPrint(Ansi.ansi().fgYellow().a(methodName).toString());
        }
        // Set the correct logging level
        if (isBuild()) {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            lc.reset(); // Disable loading from XML configuration
        } else {
            Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            LoggingLevel level = extensionContext.getTestMethod().get()
                    .getAnnotation(LoggingLevel.class);
            if (level != null) {
                root.setLevel(level.level().level);
            } else {
                root.setLevel(Level.INFO);
            }
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (CorfuRuntime.class.isAssignableFrom(parameterContext.getParameter().getType())) {
            return true;
        } else if (SMRMap.class.isAssignableFrom(parameterContext.getParameter().getType())) {
            return true;
        }
        return false;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (CorfuRuntime.class.isAssignableFrom(parameterContext.getParameter().getType())) {
            return extensionContext.getStore(ExtensionContext.Namespace
                    .create(CorfuTestExtension.class)).get(CorfuTestInstance.class,
                    CorfuTestInstance.class).getRuntimeAsParameter(parameterContext);
        } else if (SMRMap.class.isAssignableFrom(parameterContext.getParameter().getType())) {
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
            extensionContext.getRoot().getStore(ExtensionContext.Namespace
                    .create(CorfuTestExtension.class))
                    .put("method", extensionContext.getDisplayName());
            extensionContext.getRoot().getStore(ExtensionContext.Namespace
                    .create(CorfuTestExtension.class))
                    .remove("lastmethod");
        } else if (
                extensionContext.getTestMethod().isPresent()
                        && extensionContext.getParent().isPresent()
                        && extensionContext.getParent().get().getTestMethod().isPresent()
                ) {
            extensionContext.getRoot().getStore(ExtensionContext.Namespace
                    .create(CorfuTestExtension.class))
                    .put("test", extensionContext.getDisplayName());
        }
        return ConditionEvaluationResult.enabled("");
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext>
        provideTestTemplateInvocationContexts(ExtensionContext extensionContext) {

        List<CorfuTestTemplateInvocationContext> contexts = new ArrayList<>();

        if (Arrays.stream(extensionContext.getTestMethod().get().getParameters())
                            .anyMatch(x -> x.isAnnotationPresent(Iterations.class)))
        {
            contexts.addAll(Arrays.stream(CorfuConfiguration.values())
                    .flatMap(m -> Arrays.stream(Arrays.stream(extensionContext.getTestMethod().get().getParameters())
                                .filter(x -> x.isAnnotationPresent(Iterations.class))
                                .findFirst()
                                .get().getAnnotation(Iterations.class).normal())
                                .mapToObj(i -> new CorfuTestTemplateInvocationContext(m, i))
                    )
                    .collect(Collectors.toList()));
        } else {
             contexts.addAll(Arrays.stream(CorfuConfiguration.values())
                     .map(c -> new CorfuTestTemplateInvocationContext(c))
                      .collect(Collectors.toList()));
        }
        return contexts.stream()
                .map(x -> (TestTemplateInvocationContext)x);
    }

    public class CorfuTestTemplateInvocationContext implements TestTemplateInvocationContext {

        String name;
        CorfuConfiguration configuration;
        int iterations = 1;

        public CorfuTestTemplateInvocationContext(CorfuConfiguration configuration) {
            this.configuration = configuration;
            this.name = configuration.descriptiveName;
        }

        public CorfuTestTemplateInvocationContext(CorfuConfiguration configuration,
                                                  int iterations) {
            this.configuration = configuration;
            this.iterations = iterations;
            this.name = configuration.descriptiveName + " ▶ " + iterations;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return name;

        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return ImmutableList.<Extension>builder()
                    .add(
                            new BeforeTestExecutionCallback() {
                                @Override
                                public void beforeTestExecution(ExtensionContext extensionContext)
                                        throws Exception {
                                    // Generate a new CorfuTestInstance and put it in the store
                                    CorfuTestInstance ti = new CorfuTestInstance(extensionContext, configuration);
                                    extensionContext.getStore(ExtensionContext.Namespace.create
                                            (CorfuTestExtension.class))
                                            .put(CorfuTestInstance.class, ti);

                                    if (ICorfuTest.class
                                            .isAssignableFrom(extensionContext.getRequiredTestInstance().getClass())) {
                                        ((ICorfuTest) extensionContext.getRequiredTestInstance()).setUtil(
                                                new ICorfuTest.CorfuTestUtil(ti::getNewRuntime));
                                    }
                                }
                            })
                    .add(
                            new ParameterResolver() {
                                @Override
                                public boolean supportsParameter(ParameterContext parameterContext,
                                                                 ExtensionContext extensionContext)
                                        throws ParameterResolutionException {
                                    if (parameterContext.getParameter()
                                            .isAnnotationPresent(Iterations.class)) {
                                        return true;
                                    }
                                    return false;
                                }

                                @Override
                                public Object resolveParameter(ParameterContext parameterContext,
                                                               ExtensionContext extensionContext)
                                        throws ParameterResolutionException {
                                    if (parameterContext.getParameter().isAnnotationPresent(Iterations.class)) {
                                        if (!parameterContext.getParameter().getType().equals(int.class)) {
                                            throw new ParameterResolutionException("@Iterations type must be a int");
                                        }
                                        return iterations;
                                    }

                                    throw new ParameterResolutionException("Unknown parameter");
                                }
                            })
                    .build();

        }
    }


}
