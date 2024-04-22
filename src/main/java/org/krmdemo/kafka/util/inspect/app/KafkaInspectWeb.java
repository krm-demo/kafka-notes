package org.krmdemo.kafka.util.inspect.app;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.DefaultLoggingEvent;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.spi.DefaultLoggingEventBuilder;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.thymeleaf.ThymeleafProperties;
import org.springframework.core.env.Environment;
import org.springframework.http.server.RequestPath;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.util.ServletRequestPathUtils;

import java.util.*;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.list;
import static java.util.function.Function.identity;
import static org.krmdemo.kafka.util.StreamUtils.stream;
import static org.krmdemo.kafka.util.StreamUtils.toLinkedMap;

@Slf4j
@Controller
public class KafkaInspectWeb {

    @Autowired
    private Environment environment;

    @RequestMapping("/home")
    public String home(HttpServletRequest request) {
        return "index.html";
    }

//    @RequestMapping("/*.html")
//    public String thymeleafHtmlView(HttpServletRequest request) {
//        RequestPath requestPath = ServletRequestPathUtils.getParsedRequestPath(request);
//        String prefixMsg = format("thymeleafHtmlView(%s) - ", requestPath);
//        var logInfo = withPrefix(Level.INFO, prefixMsg);
//        var logDebug = withPrefix(Level.DEBUG, format("thymeleafHtmlView(%s) .. multi-line: ..\n", requestPath));
//        logInfo.log("STARTED");
//
//        Map<String, List<String>> paramsMap = stream(request.getParameterNames())
//            .collect(toLinkedMap(identity(), name -> asList(request.getParameterValues(name))));
//        logInfo.log("parameters -->" + paramsMap);
//
//        Map<String, List<String>>  headersMap = stream(request.getHeaderNames())
//            .collect(toLinkedMap(identity(), name -> list(request.getHeaders(name))));
//        logInfo.log("headers --> " + headersMap);
//
//        String prefix = environment.getProperty("spring.thymeleaf.prefix", ThymeleafProperties.DEFAULT_PREFIX);
//        String suffix = environment.getProperty("spring.thymeleaf.suffix", ThymeleafProperties.DEFAULT_SUFFIX);
//        logInfo.log("FINISHED with prefix({}) and suffix({}) - env class is {}",
//            prefix, suffix, environment.getClass());
//        return request.getPathInfo();
//
//    }

    private LoggingEventBuilder withPrefix(Level level, String msgPrefix) {
        return new DefaultLoggingEventBuilder(log, level) {
            @Override
            public void log(LoggingEvent aLoggingEvent) {
                if (aLoggingEvent instanceof DefaultLoggingEvent defLE) {
                    defLE.setMessage(msgPrefix + defLE.getMessage());
                }
                super.log(aLoggingEvent);
                this.loggingEvent = new DefaultLoggingEvent(level, logger);
            }
        };
    }
}
