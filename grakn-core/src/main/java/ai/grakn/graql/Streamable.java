/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * An interface describing something that can be streamed, or iterated over.
 * @param <T> the elements within the resulting Stream or Iterator
 *
 * @author Felix Chapman
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
