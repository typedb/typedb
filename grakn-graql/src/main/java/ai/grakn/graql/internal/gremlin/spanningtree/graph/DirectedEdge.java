/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.gremlin.spanningtree.graph;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

/**
 * An edge in a directed graph.
 *
 * @param <V> the type of the node
 * @author Jason Liu
 */
public class DirectedEdge<V> {
    public final V source;
    public final V destination;

    public DirectedEdge(V source, V destination) {
        this.source = source;
        this.destination = destination;
    }

    /**
     * @param <V> the type of the node
     */
    public static class EdgeBuilder<V> {
        public final V source;

        private EdgeBuilder(V source) {
            this.source = source;
        }

        public DirectedEdge<V> to(V destination) {
            return new DirectedEdge<>(source, destination);
        }
    }

    public static <T> EdgeBuilder<T> from(T source) {
        return new EdgeBuilder<>(source);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(source, destination);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("source", source)
                .add("destination", destination).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DirectedEdge other = (DirectedEdge) o;
        return this.source.equals(other.source) && this.destination.equals(other.destination);
    }

    //// Edge Predicates

    public static <T> Predicate<DirectedEdge<T>> hasDestination(final T node) {
        return input -> {
            assert input != null;
            return input.destination.equals(node);
        };
    }

    public static <T> Predicate<DirectedEdge<T>> competesWith(final Set<DirectedEdge<T>> required) {
        final ImmutableMap.Builder<T, T> requiredSourceByDestinationBuilder = ImmutableMap.builder();
        for (DirectedEdge<T> edge : required) {
            requiredSourceByDestinationBuilder.put(edge.destination, edge.source);
        }
        final Map<T, T> requiredSourceByDest = requiredSourceByDestinationBuilder.build();
        return input -> {
            assert input != null;
            return (requiredSourceByDest.containsKey(input.destination) &&
                    !input.source.equals(requiredSourceByDest.get(input.destination)));
        };
    }

    public static <T> Predicate<DirectedEdge<T>> isAutoCycle() {
        return input -> {
            assert input != null;
            return input.source.equals(input.destination);
        };
    }

    public static <T> Predicate<DirectedEdge<T>> isIn(final Set<DirectedEdge<T>> banned) {
        return input -> banned.contains(input);
    }
}
