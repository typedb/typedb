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
 *
 */

package grakn.core.kb.graql.planning;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An arborescence is a directed graph in which, for the root and any other vertex,
 * there is exactly one directed path between them.
 * (https://en.wikipedia.org/wiki/Arborescence_(graph_theory))
 *
 * @param <V> the type of the nodes
 */

public class Arborescence<V> {
    /**
     * In an arborescence, each node (other than the root) has exactly one parent. This is the map
     * from each node to its parent.
     */
    private final ImmutableMap<V, V> parents;

    private final V root;

    private Arborescence(ImmutableMap<V, V> parents, V root) {
        this.parents = parents;
        this.root = root;
    }

    public static <T> Arborescence<T> of(ImmutableMap<T, T> parents) {
        if (parents != null && !parents.isEmpty()) {
            HashSet<T> allParents = Sets.newHashSet(parents.values());
            allParents.removeAll(parents.keySet());
            if (allParents.size() == 1) {
                return new Arborescence<>(parents, allParents.iterator().next());
            }
        }
        return new Arborescence<>(parents, null);
    }

    public static <T> Arborescence<T> empty() {
        return Arborescence.of(ImmutableMap.<T, T>of());
    }

    public boolean contains(DirectedEdge e) {
        final Node dest = e.destination;
        return parents.containsKey(dest) && parents.get(dest).equals(e.source);
    }

    public V getRoot() {
        return root;
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
