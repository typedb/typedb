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


package grakn.core.graph.core;

import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;

/**
 * A JanusGraphEdge connects two JanusGraphVertex. It extends the functionality provided by Blueprint's Edge and
 * is a special case of a JanusGraphRelation.
 *
 * see Edge
 * see JanusGraphRelation
 * see EdgeLabel
 */
public interface JanusGraphEdge extends JanusGraphRelation, Edge {

    /**
     * Returns the edge label of this edge
     *
     * @return edge label of this edge
     */
    default EdgeLabel edgeLabel() {
        return (EdgeLabel) getType();
    }

    /**
     * Returns the vertex for the specified direction.
     * The direction cannot be Direction.BOTH.
     *
     * @param dir Direction of IN or OUT
     * @return the vertex for the specified direction
     */
    JanusGraphVertex vertex(Direction dir);

    @Override
    default JanusGraphVertex outVertex() {
        return vertex(Direction.OUT);
    }

    @Override
    default JanusGraphVertex inVertex() {
        return vertex(Direction.IN);
    }


    /**
     * Returns the vertex at the opposite end of the edge.
     *
     * @param vertex vertex on which this edge is incident
     * @return The vertex at the opposite end of the edge.
     * @throws InvalidElementException if the edge is not incident on the specified vertex
     */
    JanusGraphVertex otherVertex(Vertex vertex);


    @Override
    default Iterator<Vertex> vertices(Direction direction) {
        List<Vertex> vertices;
        if (direction == Direction.BOTH) {
            vertices = ImmutableList.of(vertex(Direction.OUT), vertex(Direction.IN));
        } else {
            vertices = ImmutableList.of(vertex(direction));
        }
        return vertices.iterator();
    }

}
