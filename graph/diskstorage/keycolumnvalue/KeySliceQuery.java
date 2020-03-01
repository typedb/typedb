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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.StaticBuffer;

/**
 * Extends SliceQuery by a key that identifies the location of the slice in the key-ring.
 */

public class KeySliceQuery extends SliceQuery {

    private final StaticBuffer key;

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        super(sliceStart, sliceEnd);
        this.key = Preconditions.checkNotNull(key);
    }

    public KeySliceQuery(StaticBuffer key, SliceQuery query) {
        super(query);
        this.key = Preconditions.checkNotNull(key);
    }

    /**
     * @return the key of this query
     */
    public StaticBuffer getKey() {
        return key;
    }

    @Override
    public KeySliceQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public KeySliceQuery updateLimit(int newLimit) {
        return new KeySliceQuery(key, this).setLimit(newLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, super.hashCode());
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
        KeySliceQuery oth = (KeySliceQuery) other;
        return key.equals(oth.key) && super.equals(oth);
    }

    public boolean subsumes(KeySliceQuery oth) {
        return key.equals(oth.key) && super.subsumes(oth);
    }

    @Override
    public String toString() {
        return String.format("KeySliceQuery(key: %s, start: %s, end: %s, limit:%d)", key, getSliceStart(), getSliceEnd(), getLimit());
    }
}
