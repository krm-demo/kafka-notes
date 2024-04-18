package org.krmdemo.kafka.util;

import java.util.*;

import static java.util.stream.Collectors.*;

/**
 * Utility-class to work with java-streams.
 */
public class StreamUtils {

    public static SortedMap<String, String> toSortedMap(Properties props) {
        return props.entrySet().stream().collect(toMap(
            e -> "" + e.getKey(), e -> "" + e.getValue(), (x, y) -> y, TreeMap::new));
    }

    private StreamUtils() {
        // prohibit the creation of utility-class instance
        throw new UnsupportedOperationException("Cannot instantiate utility-class " + getClass().getName());
    }
}
