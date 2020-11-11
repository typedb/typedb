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

package grakn.core.traversal;

import grakn.core.graph.util.Encoding;

import java.util.Arrays;
import java.util.Objects;

abstract class TraversalEdge<V extends TraversalVertex<?>> {

    private final Encoding.Edge encoding;
    private final V from;
    private final V to;
    private final boolean isTransitive;
    private final String[] labels;
    private final int hash;

    TraversalEdge(Encoding.Edge encoding, V from, V to, boolean isTransitive, String[] labels) {
        this.encoding = encoding;
        this.from = from;
        this.to = to;
        this.isTransitive = isTransitive;
        this.labels = labels;
        this.hash = Objects.hash(encoding, from, to, isTransitive, Arrays.hashCode(labels));
    }

    V from() {
        return from;
    }

    V to() {
        return to;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final TraversalEdge<?> that = (TraversalEdge<?>) object;
        return (this.encoding.equals(that.encoding) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to) &&
                this.isTransitive == that.isTransitive &&
                Arrays.equals(this.labels, that.labels));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    static class Pattern extends TraversalEdge<TraversalVertex.Pattern> {

        Pattern(Encoding.Edge encoding, TraversalVertex.Pattern from, TraversalVertex.Pattern to,
                boolean isTransitive, String[] labels) {
            super(encoding, from, to, isTransitive, labels);
        }
    }

    static class Planner extends TraversalEdge<TraversalVertex.Planner> {

        Planner(Encoding.Edge encoding, TraversalVertex.Planner from, TraversalVertex.Planner to,
                boolean isTranstive, String[] labels) {
            super(encoding, from, to, isTranstive, labels);
        }
    }

    static class Plan extends TraversalEdge<TraversalVertex.Plan> {

        Plan(Encoding.Edge encoding, TraversalVertex.Plan from, TraversalVertex.Plan to,
             boolean isTransitive, String[] labels) {
            super(encoding, from, to, isTransitive, labels);
        }
    }
}
