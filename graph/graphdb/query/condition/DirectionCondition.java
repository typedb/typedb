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

import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.graphdb.relations.CacheEdge;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Objects;


public class DirectionCondition<E extends JanusGraphRelation> extends Literal<E> {

    private final JanusGraphVertex baseVertex;
    private final Direction direction;

    public DirectionCondition(JanusGraphVertex vertex, Direction dir) {
        this.baseVertex = vertex;
        this.direction = dir;
    }

    @Override
    public boolean evaluate(E element) {
        if (direction == Direction.BOTH) return true;
        if (element instanceof CacheEdge) {
            return direction == ((CacheEdge) element).getVertexCentricDirection();
        } else if (element instanceof JanusGraphEdge) {
            return ((JanusGraphEdge) element).vertex(direction).equals(baseVertex);
        } else if (element instanceof JanusGraphVertexProperty) {
            return direction == Direction.OUT;
        }
        return false;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), direction, baseVertex);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!getClass().isInstance(other)) {
            return false;
        }

        DirectionCondition oth = (DirectionCondition) other;
        return direction == oth.direction && baseVertex.equals(oth.baseVertex);
    }

    @Override
    public String toString() {
        return "dir[" + getDirection() + "]";
    }
}
