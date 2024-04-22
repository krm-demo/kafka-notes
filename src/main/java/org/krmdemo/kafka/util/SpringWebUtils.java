package org.krmdemo.kafka.util;

import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.lang.String.format;

public class SpringWebUtils {

    public static SimpleUrlHandlerMapping getHandlerMapping(ResourceHandlerRegistry registry) {
        return invokeGetter(registry, "getHandlerMapping");
    }

    @SuppressWarnings({"unchecked"})
    public static <R> R invokeGetter(Object obj, String methodName) {
        Class<?> clazz = obj.getClass();
        try {
            Method method = clazz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            return (R)method.invoke(obj);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            String errMsg = format("could not invoke `%s.%s()`: %s",
                clazz.getSimpleName(), methodName, ex.getMessage());
            throw new IllegalStateException(errMsg, ex);
        }
    }
}
