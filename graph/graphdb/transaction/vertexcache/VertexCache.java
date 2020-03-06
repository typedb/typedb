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

package grakn.core.graph.graphdb.transaction.vertexcache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.vertices.AbstractVertex;
import grakn.core.graph.util.datastructures.Retriever;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class VertexCache {
    // volatileVertices Map contains vertices that cannot be evicted from the basic cache, we must keep a reference to them (they're either new or modified vertices)
    private final ConcurrentMap<Long, InternalVertex> volatileVertices;
    private final Cache<Long, InternalVertex> cache;

    public VertexCache(long maxCacheSize, int concurrencyLevel, int initialDirtySize) {
        volatileVertices = new ConcurrentHashMap<>(initialDirtySize);
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxCacheSize)
                .concurrencyLevel(concurrencyLevel)
                .removalListener((RemovalListener<Long, InternalVertex>) notification -> {
                    if (notification.getCause() == RemovalCause.EXPLICIT) { //Due to invalidation at the end
                        return;
                    }
                    //We get here if the entry is evicted because of size constraint or replaced through add
                    //i.e. RemovalCause.SIZE or RemovalCause.REPLACED
                    InternalVertex v = notification.getValue();
                    if (((AbstractVertex) v).isTxOpen() && (v.isModified() || v.isRemoved())) { //move vertex to volatile map if we cannot lose track of it
                        volatileVertices.putIfAbsent(notification.getKey(), v);
                    }
                })
                .build();
    }

    public boolean contains(long vertexId) {
        return cache.getIfPresent(vertexId) != null || volatileVertices.containsKey(vertexId);
    }

    public InternalVertex get(long id, Retriever<Long, InternalVertex> retriever) {
        Long vertexId = id;

        // If cached, retrieve and return
        InternalVertex vertex = cache.getIfPresent(vertexId);
        if (vertex != null) return vertex;

        // Otherwise check in the new vertices, if it's present, cache it and return it
        InternalVertex newVertex = volatileVertices.get(vertexId);
        if (newVertex != null) {
            cache.put(vertexId, newVertex); // super minor optimisation that we can remove if causes issues
            return newVertex;
        }

        // As last resort ask the retriever, cache it and return it
        InternalVertex retrieveVertex = retriever.get(vertexId);
        cache.put(vertexId, retrieveVertex);
        return retrieveVertex;
    }

    public void add(InternalVertex vertex) {
        Preconditions.checkNotNull(vertex);
        Long vertexId = vertex.longId();

        cache.put(vertexId, vertex);
        if (vertex.isNew() || vertex.hasAddedRelations()) {
            volatileVertices.put(vertexId, vertex);
        }
    }


    /**
     * Returns an iterable over all new vertices in the cache
     */
    public List<InternalVertex> getAllNew() {
        List<InternalVertex> vertices = new ArrayList<>(10);
        for (InternalVertex v : volatileVertices.values()) {
            if (v.isNew()) vertices.add(v);
        }
        return vertices;
    }

}
