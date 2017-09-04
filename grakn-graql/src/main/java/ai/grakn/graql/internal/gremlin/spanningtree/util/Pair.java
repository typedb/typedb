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

package ai.grakn.graql.internal.gremlin.spanningtree.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * 2-tuple of anything!
 *
 * @param <T> Object 1
 * @param <V> Object 2
 * @author Sam Thomson
 * @author Jason Liu
 */
public class Pair<T, V> {
    public final T first;
    public final V second;

    public Pair(T a, V b) {
        this.first = a;
        this.second = b;
    }

    /**
     * Convenience static constructor
     */
    public static <T, V> Pair<T, V> of(T a, V b) {
        return new Pair<T, V>(a, b);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Pair)) return false;
        final Pair wOther = (Pair) other;
        return Objects.equal(first, wOther.first) && Objects.equal(second, wOther.second);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("first", first)
                .add("second", second).toString();
    }
}
