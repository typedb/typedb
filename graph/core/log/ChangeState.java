/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.core.log;

import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.RelationType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

/**
 * Container interface for a set of changes against the graph caused by a particular transaction. This is passed as an argument to
 * {@link ChangeProcessor#process(JanusGraphTransaction, TransactionId, ChangeState)}
 * for the user to retrieve changed elements and act upon it.
 */
public interface ChangeState {

    /**
     * Returns all added, removed, or modified vertices when the change argument is {@link Change#ADDED},
     * {@link Change#REMOVED}, or {@link Change#ANY} respectively.
     */
    Set<JanusGraphVertex> getVertices(Change change);

    /**
     * Returns all relations that match the change state and any of the provided relation types. If no relation types
     * are specified all relations matching the state are returned.
     */
    Iterable<JanusGraphRelation> getRelations(Change change, RelationType... types);

    /**
     * Returns all edges incident on the given vertex in the given direction that match the provided change state and edge labels.
     */
    Iterable<JanusGraphEdge> getEdges(Vertex vertex, Change change, Direction dir, String... labels);


    /**
     * Returns all properties incident for the given vertex that match the provided change state and property keys.
     */
    Iterable<JanusGraphVertexProperty> getProperties(Vertex vertex, Change change, String... keys);


}
