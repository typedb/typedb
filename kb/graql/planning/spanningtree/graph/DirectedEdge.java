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

package grakn.core.kb.graql.planning.spanningtree.graph;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

/**
 * An edge in a directed graph.
 *
 */
public class DirectedEdge {
    public final Node source;
    public final Node destination;

    public DirectedEdge(Node source, Node destination) {
        this.source = source;
        this.destination = destination;
    }

    public static class EdgeBuilder {
        public final Node source;

        private EdgeBuilder(Node source) {
            this.source = source;
        }

        public DirectedEdge to(Node destination) {
            return new DirectedEdge(source, destination);
        }
    }

    public static EdgeBuilder from(Node source) {
        return new EdgeBuilder(source);
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

    public static Predicate<DirectedEdge> hasDestination(final Node node) {
        return input -> {
            Preconditions.checkNotNull(input);
            return input.destination.equals(node);
        };
    }

    public static Predicate<DirectedEdge> competesWith(final Set<DirectedEdge> required) {
        final ImmutableMap.Builder<Node, Node> requiredSourceByDestinationBuilder = ImmutableMap.builder();
        for (DirectedEdge edge : required) {
            requiredSourceByDestinationBuilder.put(edge.destination, edge.source);
        }
        final Map<Node, Node> requiredSourceByDest = requiredSourceByDestinationBuilder.build();
        return input -> {
            Preconditions.checkNotNull(input);
            return (requiredSourceByDest.containsKey(input.destination) &&
                    !input.source.equals(requiredSourceByDest.get(input.destination)));
        };
    }

    public static Predicate<DirectedEdge> isAutoCycle() {
        return input -> {
            Preconditions.checkNotNull(input);
            return input.source.equals(input.destination);
        };
    }

    public static Predicate<DirectedEdge> isIn(final Set<DirectedEdge> banned) {
        return input -> banned.contains(input);
    }
}
