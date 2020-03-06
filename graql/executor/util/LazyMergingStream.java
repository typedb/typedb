/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.executor.util;

import java.util.Collections;
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
 * <p>
 * This method converts a Stream of Streams to a flat Stream by converting each stream into an iterator internally
 * It can only be consumed once!
 * Using this method means we lose the ability to utilise Parallel Streams on the Stream returned by `flatStream()`
 */
public class LazyMergingStream<D> {
    private Stream<Stream<D>> streams;

    public LazyMergingStream(Stream<Stream<D>> streams) {
        this.streams = streams;
    }

    public Stream<D> flatStream() {
        Iterator<D> iterator = new Iterator<D>() {
            Iterator<Stream<D>> streamIterator = streams.iterator();
            Iterator<D> currentIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (currentIterator.hasNext()) {
                    return true;
                } else {
                    while (streamIterator.hasNext()) {
                        currentIterator = streamIterator.next().iterator();
                        if (currentIterator.hasNext()) {
                            return true;
                        }
                    }
                }

                return false;
            }

            @Override
            public D next() {
                return currentIterator.next();
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

}
