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

package grakn.core.graph.diskstorage;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

public interface EntryList extends List<Entry> {

    /**
     * Returns the same iterator as #iterator() with the only difference
     * that it reuses Entry objects when calling Iterator#next().
     * Hence, this method should only be used if references to Entry objects are only
     * kept and accessed until the next Iterator#next() call.
     */
    Iterator<Entry> reuseIterator();


    /**
     * Returns the total amount of bytes this entry consumes on the heap - including all object headers.
     */
    int getByteSize();


    EmptyList EMPTY_LIST = new EmptyList();

    class EmptyList extends AbstractList<Entry> implements EntryList {

        @Override
        public Entry get(int index) {
            throw new ArrayIndexOutOfBoundsException();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterator<Entry> reuseIterator() {
            return iterator();
        }

        @Override
        public int getByteSize() {
            return 0;
        }
    }

}
