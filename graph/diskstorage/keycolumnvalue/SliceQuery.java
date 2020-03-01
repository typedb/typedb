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
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.diskstorage.util.StaticArrayEntryList;
import grakn.core.graph.graphdb.query.BackendQuery;
import grakn.core.graph.graphdb.query.BaseQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Queries for a slice of data identified by a start point (inclusive) and end point (exclusive).
 * Returns all StaticBuffers that lie in this range up to the given limit.
 * <p>
 * If a SliceQuery is marked <i>static</i> it is expected that the result set does not change.
 */

public class SliceQuery extends BaseQuery implements BackendQuery<SliceQuery> {

    private final StaticBuffer sliceStart;
    private final StaticBuffer sliceEnd;

    public SliceQuery(StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        this.sliceStart = sliceStart;
        this.sliceEnd = sliceEnd;
    }

    public SliceQuery(SliceQuery query) {
        this(query.getSliceStart(), query.getSliceEnd());
        setLimit(query.getLimit());
    }

    /**
     * The start of the slice is considered to be inclusive
     *
     * @return The StaticBuffer denoting the start of the slice
     */
    public StaticBuffer getSliceStart() {
        return sliceStart;
    }

    /**
     * The end of the slice is considered to be exclusive
     *
     * @return The StaticBuffer denoting the end of the slice
     */
    public StaticBuffer getSliceEnd() {
        return sliceEnd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sliceStart, sliceEnd, getLimit());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null && !getClass().isInstance(other)) {
            return false;
        }

        SliceQuery oth = (SliceQuery) other;
        return sliceStart.equals(oth.sliceStart)
                && sliceEnd.equals(oth.sliceEnd)
                && getLimit() == oth.getLimit();
    }

    public boolean subsumes(SliceQuery oth) {
        Preconditions.checkNotNull(oth);
        if (this == oth) {
            return true;
        }

        if (oth.getLimit() > getLimit()) {
            return false;
        } else if (!hasLimit()) { //the interval must be subsumed
            return sliceStart.compareTo(oth.sliceStart) <= 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
        } else { //this the result might be cutoff due to limit, the start must be the same
            return sliceStart.compareTo(oth.sliceStart) == 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
        }
    }

    //TODO: make this more efficient by using reuseIterator() on otherResult
    public EntryList getSubset(SliceQuery otherQuery, EntryList otherResult) {
        int pos = Collections.binarySearch(otherResult, sliceStart);
        if (pos < 0) {
            pos = -pos - 1;
        }

        List<Entry> result = new ArrayList<>();
        for (; pos < otherResult.size() && result.size() < getLimit(); pos++) {
            Entry e = otherResult.get(pos);
            if (e.getColumnAs(StaticBuffer.STATIC_FACTORY).compareTo(sliceEnd) < 0) {
                result.add(e);
            } else {
                break;
            }
        }
        return StaticArrayEntryList.of(result);
    }

    public boolean contains(StaticBuffer buffer) {
        return sliceStart.compareTo(buffer) <= 0 && sliceEnd.compareTo(buffer) > 0;
    }

    public static StaticBuffer pointRange(StaticBuffer point) {
        return BufferUtil.nextBiggerBuffer(point);
    }

    @Override
    public SliceQuery setLimit(int limit) {
        Preconditions.checkArgument(!hasLimit());
        super.setLimit(limit);
        return this;
    }

    @Override
    public SliceQuery updateLimit(int newLimit) {
        return new SliceQuery(sliceStart, sliceEnd).setLimit(newLimit);
    }

}
