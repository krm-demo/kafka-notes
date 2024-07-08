package org.krmdemo;

import org.junit.jupiter.api.Test;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ITemplateResolver;
import org.thymeleaf.templateresolver.StringTemplateResolver;

import java.util.*;

import static java.lang.String.format;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;

public class ThymeLeafTest {

    @Test
    void testSimpleTemplate() {
        TemplateEngine templateEngine = new SpringTemplateEngine();
        StringTemplateResolver templateResolver = new StringTemplateResolver();
        templateResolver.setTemplateMode(TemplateMode.TEXT);
        templateEngine.setTemplateResolver(templateResolver);

        Context templateContext = new Context();
        templateContext.setVariable("a", 123);
        templateContext.setVariable("pi", Math.PI);
        templateContext.setVariable("str", format("Hello from %s!", getClass().getSimpleName()));

        Map<String, Object> myMap = new LinkedHashMap<>();
        myMap.put("propOne", "value-one");
        myMap.put("propTwo", "<b>value-two</b>");
        templateContext.setVariable("myMap", myMap);

        String templateContent = """
            Some multi-line text with placeholders:
            a = [(${a})]
            pi = [(${pi})]
            str = '[(${str})]'
            ------- myMap: --------------
            myMap --> [(${myMap})]:
            [# th:each="item : ${myMap}"]
            - [(${item})]
            [/]
            -----------------------------
            [# th:each="item, itemSt : ${myMap}" th:utext="${'- ' + item.key + ' = `' + item.value + (itemSt.last?'`.':'`;\n')}" /]
            ------- the last line -------
            """;

        String templateResult = templateEngine.process(templateContent, templateContext);
        System.out.println(templateResult);
    }

    @Test
    void testEnvVars() {
        SpringTemplateEngine templateEngine = new SpringTemplateEngine();
        StringTemplateResolver templateResolver = new StringTemplateResolver();
        templateResolver.setTemplateMode(TemplateMode.TEXT);
        templateEngine.setTemplateResolver(templateResolver);

        Context templateContext = new Context();
        templateContext.setVariable("envMap", new TreeMap<>(System.getenv()));

        String templateContent = """
            There are [(${envMap.size()})] environment variables:
            ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~[# th:each="envItem: ${envMap}"]
            [(${envItem})][/]
            ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            """;

        String templateResult = templateEngine.process(templateContent, templateContext);
        System.out.println(templateResult);
    }

    @Test
    void testSystemProps() {
        SpringTemplateEngine templateEngine = new SpringTemplateEngine();
        StringTemplateResolver templateResolver = new StringTemplateResolver();
        templateResolver.setTemplateMode(TemplateMode.TEXT);
        templateEngine.setTemplateResolver(templateResolver);

        Context templateContext = new Context();
        templateContext.setVariable("sysPropMap", toSortedMap(System.getProperties()));

        String templateContent = """
            There are [(${sysPropMap.size()})] java system-properties:
            =============================[# th:each="sysProp: ${sysPropMap}"]
            [(${sysProp})][/]
            =============================
            """;

        String templateResult = templateEngine.process(templateContent, templateContext);
        System.out.println(templateResult);
    }
}
