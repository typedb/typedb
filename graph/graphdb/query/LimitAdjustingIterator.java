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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator implementation that wraps around another iterator to iterate it up to a given limit.
 * The idea is that the wrapped iterator is based on data that is fairly expensive to retrieve (e.g. from a database).
 * As such, we don't want to retrieve all of it but "just enough". However, if more data is requested, then we want
 * the wrapped iterator to be updated (i.e. additional data be retrieved).
 * <p>
 * The limit for the wrapped iterator is updated by a factor of 2. When the iterator is updated, the iterator must be
 * iterated through to the point of the last returned element. While this may seem expensive, it is less expensive than
 * retrieving more than needed elements in the first place. However, this still means the initial currentLimit in the
 * constructor should be chosen wisely.
 */
public abstract class LimitAdjustingIterator<R> implements Iterator<R> {

    private final int maxLimit;
    private int currentLimit;
    private int count;

    private Iterator<R> iterator;

    /**
     * Initializes this iterator with the current limit and the maximum number of elements that may be retrieved from the
     * wrapped iterator.
     */
    public LimitAdjustingIterator(int maxLimit, int currentLimit) {
        Preconditions.checkArgument(currentLimit > 0 && maxLimit > 0, "Invalid limits: current [%s], max [%s]", currentLimit, maxLimit);
        this.currentLimit = currentLimit;
        this.maxLimit = maxLimit;
        this.count = 0;
        this.iterator = null;
    }

    /**
     * This returns the wrapped iterator with up to the specified number of elements.
     */
    public abstract Iterator<R> getNewIterator(int newLimit);

    @Override
    public boolean hasNext() {
        if (iterator == null) {
            iterator = getNewIterator(currentLimit);
        }
        if (count < currentLimit) {
            return iterator.hasNext();
        }
        if (currentLimit >= maxLimit) {
            return false;
        }

        //Get an iterator with an updated limit
        currentLimit = (int) Math.min(maxLimit, Math.round(currentLimit * 2.0));
        iterator = getNewIterator(currentLimit);

        /*
        We need to iterate out the iterator to the point where we last left of. This is pretty expensive and hence
        it should be ensured that the initial limit is a good guesstimate.
         */
        for (int i = 0; i < count; i++) {
            iterator.next();
        }
        return hasNext();
    }

    @Override
    public R next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        count++;
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
