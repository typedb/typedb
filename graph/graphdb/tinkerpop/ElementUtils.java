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

package grakn.core.graph.graphdb.tinkerpop;

import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ElementUtils {

    public static long getVertexId(Object id) {
        if (null == id) throw new IllegalArgumentException("Vertex ID cannot be null");

        if (id instanceof JanusGraphVertex) { //allows vertices to be "re-attached" to the current transaction
            return ((JanusGraphVertex) id).longId();
        }
        if (id instanceof Long) {
            return (Long) id;
        }
        if (id instanceof Number) {
            return ((Number) id).longValue();
        }

        // handles the case of a user passing a "detached" Vertex (DetachedVertex, StarVertex, etc).
        if (id instanceof Vertex) {
            return Long.parseLong(((Vertex) id).id().toString());
        } else {
            return Long.valueOf(id.toString());
        }
    }

    public static RelationIdentifier getEdgeId(Object id) {
        if (null == id) throw new IllegalArgumentException("Edge ID cannot be null");

        if (id instanceof JanusGraphEdge) {
            return (RelationIdentifier) ((JanusGraphEdge) id).id();
        } else if (id instanceof RelationIdentifier) {
            return (RelationIdentifier) id;
        } else if (id instanceof String) {
            return RelationIdentifier.parse((String) id);
        } else if (id instanceof long[]) {
            return RelationIdentifier.get((long[]) id);
        } else if (id instanceof int[]) {
            return RelationIdentifier.get((int[]) id);
        }

        throw new IllegalArgumentException("Edge ID format not recognised.");
    }

    // Check that ids are either:
    // - ALL Element
    // - ALL Non-Element
    public static void verifyArgsMustBeEitherIdOrElement(Object... ids) {
        int numElements = 0;
        for (Object id : ids) {
            if (id instanceof Element) numElements++;
        }
        if (numElements > 0 && numElements < ids.length) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
    }
}
