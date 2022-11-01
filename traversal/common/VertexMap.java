/*
 * Copyright (C) 2022 Vaticle
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
 *
 */

package com.vaticle.typedb.core.traversal.common;

import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Collections.unmodifiableMap;

public class VertexMap {

    private final Map<Identifier, Vertex<?, ?>> map;
    private final int hash;

    public VertexMap(Map<Identifier, Vertex<?, ?>> map) {
        this.map = unmodifiableMap(map);
        this.hash = Objects.hash(this.map);
    }

    public static VertexMap of(Map<Identifier, Vertex<?, ?>> map) {
        return new VertexMap(map);
    }

    public Map<Identifier, Vertex<?, ?>> map() {
        return map;
    }

    public Vertex<?, ?> get(Identifier id) {
        return map.get(id);
    }

    public boolean containsKey(Identifier id) {
        return map.containsKey(id);
    }

    public void forEach(BiConsumer<Identifier, Vertex<?, ?>> action) {
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
