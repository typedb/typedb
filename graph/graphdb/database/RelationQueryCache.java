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

package grakn.core.graph.graphdb.database;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.EnumMap;
import java.util.concurrent.ExecutionException;

public class RelationQueryCache {

    private final Cache<Long, CacheEntry> cache;
    private final EdgeSerializer edgeSerializer;
    private final EnumMap<RelationCategory, SliceQuery> relationTypes;

    RelationQueryCache(EdgeSerializer edgeSerializer) {
        this(edgeSerializer, 256);
    }

    public RelationQueryCache(EdgeSerializer edgeSerializer, int capacity) {
        this.edgeSerializer = edgeSerializer;
        this.cache = CacheBuilder.newBuilder().maximumSize(capacity * 3 / 2).initialCapacity(capacity).concurrencyLevel(2).build();
        relationTypes = new EnumMap<>(RelationCategory.class);
        for (RelationCategory rt : RelationCategory.values()) {
            relationTypes.put(rt, edgeSerializer.getQuery(rt, false));
        }
    }

    public SliceQuery getQuery(RelationCategory type) {
        return relationTypes.get(type);
    }

    public SliceQuery getQuery(InternalRelationType type, Direction dir) {
        CacheEntry ce;
        try {
            ce = cache.get(type.longId(), () -> new CacheEntry(edgeSerializer, type));
        } catch (ExecutionException e) {
            throw new AssertionError("Should not happen: " + e.getMessage());
        }
        return ce.get(dir);
    }

    private class CacheEntry {

        private final SliceQuery in;
        private final SliceQuery out;
        private final SliceQuery both;

        private CacheEntry(EdgeSerializer edgeSerializer, InternalRelationType t) {
            if (t.isPropertyKey()) {
                out = edgeSerializer.getQuery(t, Direction.OUT, new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                in = out;
                both = out;
            } else {
                out = edgeSerializer.getQuery(t, Direction.OUT,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                in = edgeSerializer.getQuery(t, Direction.IN,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                both = edgeSerializer.getQuery(t, Direction.BOTH,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length]);
            }
        }

        private SliceQuery get(Direction dir) {
            switch (dir) {
                case IN:
                    return in;
                case OUT:
                    return out;
                case BOTH:
                    return both;
                default:
                    throw new AssertionError("Unknown direction: " + dir);
            }
        }

    }

}
