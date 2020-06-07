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

import grakn.core.graph.diskstorage.StaticBuffer;

/**
 * A range of bytes between start and end where start is inclusive and end is exclusive.
 *
 */
public class KeyRange {

    private final StaticBuffer start;
    private final StaticBuffer end;

    public KeyRange(StaticBuffer start, StaticBuffer end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return String.format("KeyRange(left: %s, right: %s)", start, end);
    }

    public StaticBuffer getAt(int position) {
        switch(position) {
            case 0: return start;
            case 1: return end;
            default: throw new IndexOutOfBoundsException("Exceed length of 2: " + position);
        }
    }

    public StaticBuffer getStart() {
        return start;
    }

    public StaticBuffer getEnd() {
        return end;
    }
}
