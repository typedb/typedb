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

package grakn.core.graph.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.StaticBuffer;

import java.util.Objects;

/**
 * Extends a SliceQuery to express a range for columns and a range for
 * keys. Selects each key on the interval
 * {@code [keyStart inclusive, keyEnd exclusive)} for which there exists at
 * least one column between {@code [sliceStart inclusive, sliceEnd exclusive)}.
 * <p>
 * The limit of a KeyRangeQuery applies to the maximum number of columns
 * returned per key which fall into the specified slice range and NOT to the
 * maximum number of keys returned.
 */

public class KeyRangeQuery extends SliceQuery {

    private final StaticBuffer keyStart;
    private final StaticBuffer keyEnd;

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        super(sliceStart, sliceEnd);
        this.keyStart = Preconditions.checkNotNull(keyStart);
        this.keyEnd = Preconditions.checkNotNull(keyEnd);
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, SliceQuery query) {
        super(query);
        this.keyStart = Preconditions.checkNotNull(keyStart);
        this.keyEnd = Preconditions.checkNotNull(keyEnd);
    }


    public StaticBuffer getKeyStart() {
        return keyStart;
    }

    public StaticBuffer getKeyEnd() {
        return keyEnd;
    }

    @Override
    public KeyRangeQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public KeyRangeQuery updateLimit(int newLimit) {
        return new KeyRangeQuery(keyStart, keyEnd, this).setLimit(newLimit);
    }


    @Override
    public int hashCode() {
        return Objects.hash(keyStart, keyEnd, super.hashCode());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        } else if (!getClass().isInstance(other)) {
            return false;
        }
        KeyRangeQuery oth = (KeyRangeQuery) other;
        return keyStart.equals(oth.keyStart) && keyEnd.equals(oth.keyEnd) && super.equals(oth);
    }

    public boolean subsumes(KeyRangeQuery oth) {
        return super.subsumes(oth) && keyStart.compareTo(oth.keyStart) <= 0 && keyEnd.compareTo(oth.keyEnd) >= 0;
    }

    @Override
    public String toString() {
        return String.format("KeyRangeQuery(start: %s, end: %s, columns:[start: %s, end: %s], limit=%d)",
                keyStart,
                keyEnd,
                getSliceStart(),
                getSliceEnd(),
                getLimit());
    }
}
