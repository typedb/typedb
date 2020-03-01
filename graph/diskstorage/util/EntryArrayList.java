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

package grakn.core.graph.diskstorage.util;

import com.google.common.collect.Iterators;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class EntryArrayList extends ArrayList<Entry> implements EntryList {

    public static final int ENTRY_SIZE_ESTIMATE = 256;

    public EntryArrayList() {
        super();
    }

    public EntryArrayList(Collection<? extends Entry> c) {
        super(c);
    }

    public static EntryArrayList of(Iterable<? extends Entry> i) {
        // This is adapted from Guava's Lists.newArrayList implementation
        EntryArrayList result;
        if (i instanceof Collection) {
            // Let ArrayList's sizing logic work, if possible
            result = new EntryArrayList((Collection) i);
        } else {
            // Unknown size
            result = new EntryArrayList();
            Iterators.addAll(result, i.iterator());
        }
        return result;
    }

    @Override
    public Iterator<Entry> reuseIterator() {
        return iterator();
    }

    /**
     * This implementation is an inexact estimate.
     * It's just the product of a constant (#ENTRY_SIZE_ESTIMATE times the array size.
     * The exact size could be calculated by iterating over the list and summing the remaining
     * size of each StaticBuffer in each Entry.
     *
     * @return crude approximation of actual size
     */
    @Override
    public int getByteSize() {
        return size() * ENTRY_SIZE_ESTIMATE;
    }
}
