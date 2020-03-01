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
 */

package grakn.core.graph.graphdb.query;

import grakn.core.graph.core.JanusGraphElement;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wraps around a result set iterator to return up to the specified limit number of elements
 * and implement the Iterator#remove() method based on element's remove method.
 */
public class ResultSetIterator<R extends JanusGraphElement> implements Iterator<R> {

    private final Iterator<R> iterator;
    private final int limit;

    private R current;
    private R next;
    private int count;


    public ResultSetIterator(Iterator<R> inner, int limit) {
        this.iterator = inner;
        this.limit = limit;
        count = 0;

        this.current = null;
        this.next = nextInternal();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    private R nextInternal() {
        R r = null;
        if (count < limit && iterator.hasNext()) {
            r = iterator.next();
        }
        return r;
    }

    @Override
    public R next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        current = next;
        count++;
        next = nextInternal();
        return current;
    }

    @Override
    public void remove() {
        if (current != null) {
            current.remove();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static <R extends JanusGraphElement> Iterable<R> wrap(Iterable<R> inner, int limit) {
        return () -> new ResultSetIterator<>(inner.iterator(), limit);
    }

}
