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

package grakn.core.graph.diskstorage.keycolumnvalue.keyvalue;

import grakn.core.graph.diskstorage.StaticBuffer;

/**
 * Representation of a (key,value) pair.
 */

public class KeyValueEntry {

    private final StaticBuffer key;
    private final StaticBuffer value;

    public KeyValueEntry(StaticBuffer key, StaticBuffer value) {
        this.key = key;
        this.value = value;
    }

    public StaticBuffer getKey() {
        return key;
    }

    public StaticBuffer getValue() {
        return value;
    }

}
