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

package grakn.core.common.util;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Union-Find Data Structure
 *
 * @param <V> the type of the values stored
 */
public class Partition<V> {
    private final Map<V, V> parents;
    private final Map<V, Integer> ranks;

    private Partition(Map<V, V> parents, Map<V, Integer> ranks) {
        this.parents = parents;
        this.ranks = ranks;
    }

    /**
     * Constructs a new partition of singletons
     */
    public static <T> Partition<T> singletons(Collection<T> nodes) {
        final Map<T, T> parents = Maps.newHashMap();
        final Map<T, Integer> ranks = Maps.newHashMap();
        for (T node : nodes) {
            parents.put(node, node); // each node is its own head
            ranks.put(node, 0); // every node has depth 0 to start
        }
        return new Partition<T>(parents, ranks);
    }

    /**
     * Find the representative for the given item
     */
    public V componentOf(V i) {
        final V parent = parents.getOrDefault(i, i);
        if (parent.equals(i)) {
            return i;
        } else {
            // collapse, so next lookup is O(1)
            parents.put(i, componentOf(parent));
        }
        return parents.get(i);
    }

    /**
     * Merges the given components and returns the representative of the new component
     */
    public V merge(V a, V b) {
        final V aHead = componentOf(a);
        final V bHead = componentOf(b);
        if (aHead.equals(bHead)) return aHead;
        // add the shorter tree underneath the taller tree
        final int aRank = ranks.getOrDefault(aHead, 0);
        final int bRank = ranks.getOrDefault(bHead, 0);
        if (aRank > bRank) {
            parents.put(bHead, aHead);
            return aHead;
        } else if (bRank > aRank) {
            parents.put(aHead, bHead);
            return bHead;
        } else {
            // whoops, the tree got taller
            parents.put(bHead, aHead);
            ranks.put(aHead, aRank + 1);
            return aHead;
        }
    }

    /**
     * Determines whether the two items are in the same component or not
     */
    public boolean sameComponent(V a, V b) {
        return componentOf(a).equals(componentOf(b));
    }

    public Set<V> getNodes() {
        return parents.keySet();
    }
}
