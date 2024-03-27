/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.common;

import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Collections.unmodifiableMap;

public class VertexMap {

    private final Map<Retrievable, Vertex<?, ?>> map;
    private final int hash;

    public VertexMap(Map<Retrievable, Vertex<?, ?>> map) {
        this.map = unmodifiableMap(map);
        this.hash = Objects.hash(this.map);
    }

    public static VertexMap of(Map<Retrievable, Vertex<?, ?>> map) {
        return new VertexMap(map);
    }

    public Map<Retrievable, Vertex<?, ?>> map() {
        return map;
    }

    public Vertex<?, ?> get(Retrievable id) {
        return map.get(id);
    }

    public boolean containsKey(Retrievable id) {
        return map.containsKey(id);
    }

    public void forEach(BiConsumer<Retrievable, Vertex<?, ?>> action) {
        map.forEach(action);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VertexMap that = (VertexMap) o;
        return this.map.equals(that.map);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
