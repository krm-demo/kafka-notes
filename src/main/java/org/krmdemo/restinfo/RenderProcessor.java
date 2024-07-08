package org.krmdemo.restinfo;

import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templateresolver.ITemplateResolver;
import org.thymeleaf.templateresolver.StringTemplateResolver;

public class RenderProcessor {

    public String render() {
        TemplateEngine templateEngine = new TemplateEngine();
        ITemplateResolver templateResolver = new StringTemplateResolver();
        templateEngine.setTemplateResolver(templateResolver);

        return templateEngine.process("la-la-la", new Context());
    }
}
