/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin.spanningtree;

import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @param <V> the type of the nodes
 * @author Jason Liu
 */

public class Arborescence<V> {
    /**
     * In an arborescence, each node (other than the root) has exactly one parent. This is the map
     * from each node to its parent.
     */
    private final ImmutableMap<V, V> parents;

    private V root = null;

    private Arborescence(ImmutableMap<V, V> parents) {
        this.parents = parents;
    }

    public static <T> Arborescence<T> of(ImmutableMap<T, T> parents) {
        Arborescence<T> arborescence = new Arborescence<>(parents);
        if (parents != null && !parents.isEmpty()) {
            HashSet<T> allParents = Sets.newHashSet(parents.values());
            allParents.removeAll(parents.keySet());
            if (!allParents.isEmpty()) {
                arborescence.root = allParents.iterator().next();
            }
        }
        return arborescence;
    }

    public static <T> Arborescence<T> empty() {
        return Arborescence.of(ImmutableMap.<T, T>of());
    }

    public boolean contains(DirectedEdge<V> e) {
        final V dest = e.destination;
        return parents.containsKey(dest) && parents.get(dest).equals(e.source);
    }

    public V getRoot() {
        return root;
    }

    public int size() {
        return parents.size();
    }

    public ImmutableMap<V, V> getParents() {
        return parents;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        parents.forEach((key, value) -> stringBuilder.append(value).append(" -> ").append(key).append("; "));
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        final Arborescence that = (Arborescence) other;
        Set<Map.Entry<V, V>> myEntries = parents.entrySet();
        Set thatEntries = that.parents.entrySet();
        return myEntries.size() == thatEntries.size() && myEntries.containsAll(thatEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(parents);
    }
}
