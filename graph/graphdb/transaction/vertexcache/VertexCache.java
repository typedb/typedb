// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.transaction.vertexcache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.vertices.AbstractVertex;
import org.janusgraph.util.datastructures.Retriever;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class VertexCache {
    // volatileVertices Map contains vertices that cannot be evicted from the basic cache, we must keep a reference to them (they're either new or modified vertices)
    private final ConcurrentMap<Long, InternalVertex> volatileVertices;
    private final Cache<Long, InternalVertex> cache;

    public VertexCache(long maxCacheSize, int concurrencyLevel, int initialDirtySize) {
        volatileVertices = new NonBlockingHashMapLong<>(initialDirtySize);
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxCacheSize)
                .concurrencyLevel(concurrencyLevel)
                .removalListener((RemovalListener<Long, InternalVertex>) notification -> {
                    if (notification.getCause() == RemovalCause.EXPLICIT) { //Due to invalidation at the end
                        return;
                    }
                    //Should only get evicted based on size constraint or replaced through add
                    assert (notification.getCause() == RemovalCause.SIZE || notification.getCause() == RemovalCause.REPLACED) : "Cause: " + notification.getCause();
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
