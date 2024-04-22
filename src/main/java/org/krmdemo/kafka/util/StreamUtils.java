package org.krmdemo.kafka.util;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.*;

/**
 * Utility-class to work with java-streams.
 */
public class StreamUtils {

    /**
     * @param iter iterator over elements of type {@link T}
     * @return ordered stream of elements for {@link Iterator}
     * @param <T> type of elements
     */
    public static <T> Stream<T> stream(Iterator<T> iter) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
    }
    /**
     * @param enumeration {@link Enumeration} of type {@link T}
     * @return ordered stream of elements in {@link Enumeration}
     * @param <T> type of elements
     */
    public static <T> Stream<T> stream(Enumeration<T> enumeration) {
        return stream(enumeration.asIterator());
    }

    /**
     * The same as {@link Collectors#toMap}, but produces {@link LinkedHashMap} and ignore duplicates.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @return a {@code Collector} which collects elements into a {@link SequencedMap}
     * whose keys are the result of applying a key mapping function to the input
     * elements, and whose values are the result of applying a value mapping
     * function to all input elements equal to the key and combining them
     * using the merge function
     */
    public static <T, K, U>
    Collector<T, ?, SequencedMap<K,U>>
    toLinkedMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
        return Collectors.toMap(keyMapper, valueMapper, StreamUtils::mergeIgnore, LinkedHashMap::new);
    }

    /**
     * The same as {@link Collectors#toMap}, but produces {@link TreeMap} and ignore duplicates.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @return a {@code Collector} which collects elements into a {@link SortedMap}
     * whose keys are the result of applying a key mapping function to the input
     * elements, and whose values are the result of applying a value mapping
     * function to all input elements equal to the key and combining them
     * using the merge function
     */
    public static <T, K, U>
    Collector<T, ?, SortedMap<K,U>>
    toSortedMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
        return toSortedMap(keyMapper, valueMapper, StreamUtils::mergeIgnore);
    }

    /**
     * The same as {@link #toSortedMap(Function, Function)}, but allows to handle duplicates with merge-function.
     *
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param keyMapper a mapping function to produce keys
     * @param valueMapper a mapping function to produce values
     * @param mergeFunction a merge function, used to resolve collisions between values
     *                      associated with the same key, as supplied to {@link Map#merge(Object, Object, BiFunction)}
     * @return a {@code Collector} which collects elements into a {@link SortedMap}
     * whose keys are the result of applying a key mapping function to the input
     * elements, and whose values are the result of applying a value mapping
     * function to all input elements equal to the key and combining them
     * using the merge function
     */
    public static <T, K, U>
    Collector<T, ?, SortedMap<K,U>>
    toSortedMap(Function<? super T, ? extends K> keyMapper,
                Function<? super T, ? extends U> valueMapper,
                BinaryOperator<U> mergeFunction) {
        return Collectors.toMap(keyMapper, valueMapper, mergeFunction, TreeMap::new);
    }

    public static SortedMap<String, String> toSortedMap(Properties props) {
        return props.entrySet().stream().collect(toMap(
            e -> "" + e.getKey(), e -> "" + e.getValue(), (x, y) -> y, TreeMap::new));
    }

    private static <V> V mergeIgnore(V x, V y) { return y; }

    private StreamUtils() {
        // prohibit the creation of utility-class instance
        throw new UnsupportedOperationException("Cannot instantiate utility-class " + getClass().getName());
    }
}
