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

import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;

import java.util.List;

/**
 * An edge, together with a list of edges that can't be in the final answer if 'edge' is.
 *
 */
public class ExclusiveEdge implements Comparable<ExclusiveEdge> {
    public final DirectedEdge edge;
    public final List<DirectedEdge> excluded;
    public final double weight;

    private ExclusiveEdge(DirectedEdge edge, List<DirectedEdge> excluded, double weight) {
        this.edge = edge;
        this.excluded = excluded;
        this.weight = weight;
    }

    public static ExclusiveEdge of(DirectedEdge edge, List<DirectedEdge> excluded, double weight) {
        return new ExclusiveEdge(edge, excluded, weight);
    }

    @Override
    public int compareTo(ExclusiveEdge exclusiveEdge) {
        return Double.compare(weight, exclusiveEdge.weight);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ExclusiveEdge that = (ExclusiveEdge) o;
        return edge != null && edge.equals(that.edge) &&
                excluded != null && excluded.equals(that.excluded) &&
                Double.compare(weight, that.weight) == 0;
    }

    @Override
    public int hashCode() {
        int result = edge != null ? edge.hashCode() : 0;
        result = result * 31 + (excluded != null ? excluded.hashCode() : 0);
        result = result * 31 + Double.valueOf(weight).hashCode();
        return result;
    }
}
