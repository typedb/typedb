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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.StaticBuffer;

/**
 * Class representing a (key, column) pair.
 */
public class KeyColumn {

    private final StaticBuffer key;
    private final StaticBuffer col;
    private int cachedHashCode;

    public KeyColumn(StaticBuffer key, StaticBuffer col) {
        this.key = Preconditions.checkNotNull(key);
        this.col = Preconditions.checkNotNull(col);
    }

    public StaticBuffer getKey() {
        return key;
    }

    public StaticBuffer getColumn() {
        return col;
    }

    @Override
    public int hashCode() {
        // if the hashcode is needed frequently, we should store it
        if (0 != cachedHashCode) {
            return cachedHashCode;
        }

        int prime = 31;
        int result = 1;
        result = prime * result + col.hashCode();
        result = prime * result + key.hashCode();

        // This is only thread-safe because cachedHashCode is an int and not a long
        cachedHashCode = result;

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KeyColumn other = (KeyColumn) obj;
        return other.key.equals(key) && other.col.equals(col);
    }

    @Override
    public String toString() {
        return "KeyColumn [k=0x" + key.toString() +
                ", c=0x" + col.toString() + "]";
    }
}
