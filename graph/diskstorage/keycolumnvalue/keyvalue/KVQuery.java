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
import grakn.core.graph.graphdb.query.BaseQuery;

import java.util.function.Predicate;

/**
 * A query against a OrderedKeyValueStore. Retrieves all the results that lie between start (inclusive) and
 * end (exclusive) which satisfy the filter. Returns up to the specified limit number of key-value pairs KeyValueEntry.
 */
public class KVQuery extends BaseQuery {

    private final StaticBuffer start;
    private final StaticBuffer end;
    private final Predicate<StaticBuffer> keyFilter;

    public KVQuery(StaticBuffer start, StaticBuffer end) {
        this(start, end, BaseQuery.NO_LIMIT);
    }

    public KVQuery(StaticBuffer start, StaticBuffer end, int limit) {
        this(start, end, (staticBuffer) -> true, limit);
    }

    public KVQuery(StaticBuffer start, StaticBuffer end, Predicate<StaticBuffer> keyFilter, int limit) {
        super(limit);
        this.start = start;
        this.end = end;
        this.keyFilter = keyFilter;
    }

    public StaticBuffer getStart() {
        return start;
    }

    public StaticBuffer getEnd() {
        return end;
    }

    public KeySelector getKeySelector() {
        return new KeySelector(keyFilter, getLimit());
    }


}
