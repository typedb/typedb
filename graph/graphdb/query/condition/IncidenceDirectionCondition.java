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
 */

package grakn.core.graph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Objects;


public class IncidenceDirectionCondition<E extends JanusGraphRelation> extends Literal<E> {

    private final Direction direction;
    private final JanusGraphVertex otherVertex;

    public IncidenceDirectionCondition(Direction direction, JanusGraphVertex otherVertex) {
        Preconditions.checkNotNull(direction);
        Preconditions.checkNotNull(otherVertex);
        this.direction = direction;
        this.otherVertex = otherVertex;
    }

    @Override
    public boolean evaluate(E relation) {
        return relation.isEdge() && ((JanusGraphEdge) relation).vertex(direction).equals(otherVertex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), direction, otherVertex);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || !getClass().isInstance(other)) {
            return false;
        }

        IncidenceDirectionCondition oth = (IncidenceDirectionCondition) other;
        return direction == oth.direction && otherVertex.equals(oth.otherVertex);
    }

    @Override
    public String toString() {
        return "incidence[" + direction + "-" + otherVertex + "]";
    }
}
