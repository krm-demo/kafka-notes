package org.krmdemo.kafka.util.inspect.cmd;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;

import java.time.ZonedDateTime;
import java.util.*;

import static org.krmdemo.kafka.util.DumpUtils.dumpCmd;
import static org.krmdemo.kafka.util.DumpUtils.dumpProps;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;

/**
 * A simple Spring-Boot command-line application, that dumps to standard output
 * the same information as 'hello.sh' script and 'Hello.java'.
 */
@Profile("dev")
@SpringBootApplication
public class KafkaInspectCmd implements CommandLineRunner {

    @Value("${spring.application.name}")
    private String applicationName;

    @Override
    public void run(String... args) {
        System.out.printf("Spring-Boot-CLI application '%s' started at %s\n", applicationName, ZonedDateTime.now());
        System.out.println("=================================================");
        System.out.println("runtime-environment on start-up:");
        System.out.println("-------------------------------------------------");
        // for Windows the command "ver" should be used
        dumpCmd("uname", "-s"); // --kernel-name
        dumpCmd("uname", "-r"); // --kernel-release
        dumpCmd("uname", "-o"); // --operating-system
        dumpCmd("uname", "-m"); // --machine
        dumpCmd("uname", "-p"); // --processor
        dumpCmd("uname", "-n"); // --nodename
        dumpCmd("uname", "-i"); // --hardware-platform
        dumpCmd("uname", "-a"); // --all
        dumpProps(new TreeMap<>(System.getenv()));
        System.out.println("=================================================");
        System.out.println("java system-properties on start-up:");
        System.out.println("-------------------------------------------------");
        dumpProps(toSortedMap(System.getProperties()));
        System.out.println("=================================================");

        // TODO: dump the rest and move the logic to a proper classes and packages
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(KafkaInspectCmd.class, args)));
    }
}
