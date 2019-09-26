package grakn.core.graql.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java 8 (before u222) and Java 9 do not support fully lazy `flatMap` operations on Streams
 * Two options if we do not want to mandate a JVM version:
 * - Vavr (java functional library) has an alternative Stream implementation that lets us convert Java to Vavr streams,
 * which then implement flatMap lazily: downside - converting to Vavr stream consumes a single `.next()`
 * which means the laziness is partially lost
 * - Custom stream merging: we implement the flatmap functionality by going through the internal `.iterator()`
 * functionality available. This also avoids an external dependency and some overhead
 */
public class LazyMergingStream<D> {
    private Stream<Stream<D>> streams;

    public LazyMergingStream(Stream<Stream<D>> streams) {
        this.streams = streams;
    }

    public Stream<D> flatStream() {
        Iterator<D> iterator = new Iterator<D>() {
            Iterator<Stream<D>> streamIterator = streams.iterator();
            Stream<D> currentStream = streamIterator.next();
            Iterator<D> currentIterator = currentStream.iterator();

            @Override
            public boolean hasNext() {
                if (currentIterator.hasNext()) {
                    return true;
                } else {
                    if (streamIterator.hasNext()) {
                        currentIterator = streamIterator.next().iterator();
                        return currentIterator.hasNext();
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public D next() {
                return currentIterator.next();
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

}
