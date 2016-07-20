package io.mindmaps.graql.api.query;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * An interface describing something that can be streamed, or iterated over.
 * @param <T> the elements within the resulting Stream or Iterator
 */
@FunctionalInterface
public interface Streamable<T> extends Iterable<T> {

    @Override
    default Iterator<T> iterator() {
        return stream().iterator();
    }

    /**
     * @return a stream of elements
     */
    Stream<T> stream();

    /**
     * @return a parallel stream of elements
     */
    default Stream<T> parallelStream() {
        return stream().parallel();
    }
}
