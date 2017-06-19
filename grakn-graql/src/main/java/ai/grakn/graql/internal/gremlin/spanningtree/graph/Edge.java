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

package ai.grakn.graql.internal.gremlin.spanningtree.graph;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

/**
 * An edge in a directed graph.
 *
 * @param <V> the type of the node
 * @author Sam Thomson
 */
public class Edge<V> {
    public final V source;
    public final V destination;

    public Edge(V source, V destination) {
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

        public Edge<V> to(V destination) {
            return new Edge<V>(source, destination);
        }
    }

    public static <T> EdgeBuilder<T> from(T source) {
        return new EdgeBuilder<T>(source);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(source, destination);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("source", source)
                .add("destination", destination).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Edge other = (Edge) obj;
        return this.source == other.source && this.destination == other.destination;
    }

    //// Edge Predicates

    public static <T> Predicate<Edge<T>> hasDestination(final T node) {
        return new Predicate<Edge<T>>() {
            @Override
            public boolean apply(Edge<T> input) {
                assert input != null;
                return input.destination.equals(node);
            }
        };
    }

    public static <T> Predicate<Edge<T>> competesWith(final Set<Edge<T>> required) {
        final ImmutableMap.Builder<T, T> requiredSourceByDestinationBuilder = ImmutableMap.builder();
        for (Edge<T> edge : required) {
            requiredSourceByDestinationBuilder.put(edge.destination, edge.source);
        }
        final Map<T, T> requiredSourceByDest = requiredSourceByDestinationBuilder.build();
        return new Predicate<Edge<T>>() {
            @Override
            public boolean apply(Edge<T> input) {
                assert input != null;
                return (requiredSourceByDest.containsKey(input.destination) &&
                        !input.source.equals(requiredSourceByDest.get(input.destination)));
            }
        };
    }

    public static <T> Predicate<Edge<T>> isAutoCycle() {
        return new Predicate<Edge<T>>() {
            @Override
            public boolean apply(Edge<T> input) {
                assert input != null;
                return input.source.equals(input.destination);
            }
        };
    }

    public static <T> Predicate<Edge<T>> isIn(final Set<Edge<T>> banned) {
        return new Predicate<Edge<T>>() {
            @Override
            public boolean apply(Edge<T> input) {
                return banned.contains(input);
            }
        };
    }
}
