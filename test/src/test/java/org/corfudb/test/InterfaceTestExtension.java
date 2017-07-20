package org.corfudb.test;

import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

/**
 * Created by mwei on 7/18/17.
 */
public class InterfaceTestExtension implements ParameterResolver {

    Optional<ParameterizedType> getTestInterface(ParameterContext parameterContext) {
        Optional<Class<?>> raw = Arrays.stream(parameterContext.getDeclaringExecutable()
                        .getDeclaringClass().getInterfaces())
                        .filter(x -> x.isAnnotationPresent(TestableInterface.class))
                        .findFirst();
        return raw.isPresent() ?
                Arrays.stream(parameterContext.getDeclaringExecutable().getDeclaringClass()
                        .getAnnotatedInterfaces())
                        .filter(x -> ParameterizedType.class.isInstance(x.getType()))
                        .map(x -> (ParameterizedType) x.getType())
                        .filter(x -> ((Class)x.getRawType()).isAssignableFrom(raw.get()))
                        .findFirst()
                : Optional.empty();

    }
    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Optional<ParameterizedType> testInterface = getTestInterface(parameterContext);
        if (testInterface.isPresent()) {
            return Arrays.stream(testInterface.get().getActualTypeArguments())
                    .anyMatch(x -> parameterContext.getParameter().getParameterizedType()
                            .getTypeName().equals(x.getTypeName()));
        }
        return false;
    }

    public Type mapTypeParameter(ParameterizedType original, Class<?> raw, Type toMap) {
        int typeIndex =  Arrays.asList(original.getActualTypeArguments()).indexOf(toMap);
        if (typeIndex == -1) {
            throw new IllegalArgumentException("Type parameter " + toMap + " not present");
        }
        return raw.getTypeParameters()[typeIndex];
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Optional<ParameterizedType> testInterface = getTestInterface(parameterContext);
        if (testInterface.isPresent()) {
            Optional<Method> generator = Arrays.stream(((Class)testInterface.get().getRawType())
                    .getDeclaredMethods())
                    .filter(x -> x.getGenericReturnType().getTypeName()
                            .equals(
                                    mapTypeParameter(testInterface.get(),
                                            ((Class)testInterface.get().getRawType()),
                                            parameterContext.getParameter()
                                    .getParameterizedType()).getTypeName()))
                    .filter(x -> parameterContext.getParameter().isAnnotationPresent(InterfaceParameter.class) ?
                              x.getParameterCount() == 2 // TODO: check actual signature
                            : x.getParameterCount() == 0)
                    .findFirst();
            if (generator.isPresent() && parameterContext.getTarget().isPresent()) {
                try {
                    if (parameterContext.getParameter().isAnnotationPresent(InterfaceParameter.class)) {
                        InterfaceParameter param = parameterContext.getParameter().getAnnotation(InterfaceParameter.class);
                        return generator.get().invoke(parameterContext.getTarget().get(), param.index(), param.existing());
                    } else {
                        return generator.get().invoke(parameterContext.getTarget().get());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}
