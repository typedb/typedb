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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.StaticBuffer;

import java.util.function.Predicate;

/**
 * A KeySelector utility that can be generated out of a given KVQuery
 */
public class KeySelector {

    private final Predicate<StaticBuffer> keyFilter;
    private final int limit;
    private int count;

    public KeySelector(Predicate<StaticBuffer> keyFilter, int limit) {
        Preconditions.checkArgument(limit > 0, "The count limit needs to be positive. Given: " + limit);
        Preconditions.checkArgument(keyFilter != null);
        this.keyFilter = keyFilter;
        this.limit = limit;
        count = 0;
    }

    public static KeySelector of(int limit) {
        return new KeySelector((x) -> true, limit);
    }

    public boolean include(StaticBuffer key) {
        if (keyFilter.test(key)) {
            count++;
            return true;
        } else return false;
    }

    public boolean reachedLimit() {
        return count >= limit;
    }

}
