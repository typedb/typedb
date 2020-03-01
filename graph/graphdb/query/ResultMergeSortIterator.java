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

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ResultMergeSortIterator<R> implements Iterator<R> {

    private final Iterator<R> first;
    private final Iterator<R> second;
    private final Comparator<R> comp;
    private final boolean filterDuplicates;

    private R nextFirst;
    private R nextSecond;
    private R next;

    public ResultMergeSortIterator(Iterator<R> first, Iterator<R> second, Comparator<R> comparator, boolean filterDuplicates) {
        Preconditions.checkNotNull(first);
        Preconditions.checkNotNull(second);
        Preconditions.checkNotNull(comparator);
        this.first = first;
        this.second = second;
        this.comp = comparator;
        this.filterDuplicates = filterDuplicates;

        nextFirst = null;
        nextSecond = null;
        next = nextInternal();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public R next() {
        if (!hasNext()) throw new NoSuchElementException();
        R current = next;
        next = null;
        do {
            next = nextInternal();
        } while (next != null && filterDuplicates && comp.compare(current, next) == 0);
        return current;
    }

    private R nextInternal() {
        if (nextFirst == null && first.hasNext()) {
            nextFirst = first.next();
        }
        if (nextSecond == null && second.hasNext()) {
            nextSecond = second.next();
        }
        R result;
        if (nextFirst == null && nextSecond == null) {
            return null;
        } else if (nextFirst == null) {
            result = nextSecond;
            nextSecond = null;
        } else if (nextSecond == null) {
            result = nextFirst;
            nextFirst = null;
        } else {
            //Compare
            int c = comp.compare(nextFirst, nextSecond);
            if (c <= 0) {
                result = nextFirst;
                nextFirst = null;
            } else {
                result = nextSecond;
                nextSecond = null;
            }
        }
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static <R> Iterable<R> mergeSort(Iterable<R> first, Iterable<R> second, Comparator<R> comparator, boolean filterDuplicates) {
        return () -> new ResultMergeSortIterator<>(first.iterator(), second.iterator(), comparator, filterDuplicates);
    }


}
