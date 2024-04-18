package org.krmdemo.kafka.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import static java.util.Arrays.*;
import static java.util.stream.Collectors.*;

/**
 * Utility class to dump the data into standard output and logs.
 */
public class DumpUtils {

    /**
     * Sort and dump the key-value pairs per each line in <code>"%s --> '%s'"</code> format.
     * @param props key-value pairs as {@link Map}
     */
    public static void dumpProps(Map<String, String> props) {
        props.forEach((propName, propValue) -> System.out.printf("%s --> '%s'\n", propName, propValue));
    }

    /**
     * Dump the result (standard output and error) of a command-line.
     * @param cmd a command with arguments to execute as a child process
     */
    public static void dumpCmd(String... cmd) {
        String commandLine = stream(cmd).collect(joining(" ", "$(", ")"));
        try {
            Process pr = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            BufferedReader br = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            System.out.printf("%s = '%s'", commandLine, br.lines().collect(joining(" :\\n: ")));
            br.close();
            int exitCode = pr.waitFor();
            System.out.println(exitCode == 0 ? "" : " exitCode " + exitCode);
        } catch (Exception ex) {
            System.err.printf("could not execute the command-line: %s\n", commandLine);
            ex.printStackTrace(System.err);
            System.exit(-111);
        }
    }

    private DumpUtils() {
        // prohibit the creation of utility-class instance
        throw new UnsupportedOperationException("Cannot instantiate utility-class " + getClass().getName());
    }
}
