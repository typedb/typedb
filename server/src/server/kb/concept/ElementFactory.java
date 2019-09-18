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

package grakn.core.server.kb.concept;

import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.Shard;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;

/**
 * Constructs Concepts And Edges
 * This class turns Tinkerpop Vertex and Edge
 * into Grakn Concept and EdgeElement.
 * Construction is only successful if the vertex and edge properties contain the needed information.
 * A concept must include a label which is a Schema.BaseType.
 * An edge must include a label which is a Schema.EdgeLabel.
 */
public final class ElementFactory {
    private final JanusGraphTransaction janusTx;

    @Nullable
    private GraphTraversalSource graphTraversalSource = null;

    public ElementFactory(JanusGraphTransaction janusTransaction) {
        this.janusTx = janusTransaction;
    }

    public Vertex getVertexWithProperty(Schema.VertexProperty key, Object value) {
        Iterator<Vertex> vertices = getTinkerTraversal().V().has(key.name(), value);
        if (vertices.hasNext()) {
            return vertices.next();
        }
        return null;
    }

    public Vertex getVertexWithId(String id) {
        Iterator<Vertex> vertices = getTinkerTraversal().V(id);
        if (vertices.hasNext()) {
            return vertices.next();
        }
        return null;
    }

    public EdgeElement getEdgeElementWithId(String id) {
        GraphTraversal<Edge, Edge> traversal = getTinkerTraversal().E(id);
        if (traversal.hasNext()) {
            return buildEdgeElement(traversal.next());
        }
        return null;
    }


    // ---------------------------------------- Non Concept Construction -----------------------------------------------
    public EdgeElement buildEdgeElement(Edge edge) {
        return new EdgeElement(this, edge);
    }


    public Shard createShard(ConceptImpl shardOwner, VertexElement vertexElement) {
        return new Shard(shardOwner, vertexElement);
    }

    public Shard getShard(Vertex vertex) {
        return new Shard(buildVertexElement(vertex));
    }

    public Shard getShard(VertexElement vertexElement) {
        return new Shard(vertexElement);
    }

    /**
     * Creates a new Vertex in the graph and builds a VertexElement which wraps the newly created vertex
     *
     * @param baseType baseType of newly created Vertex
     * @return VertexElement
     */
    public VertexElement addVertexElement(Schema.BaseType baseType) {
        Vertex vertex = janusTx.addVertex(baseType.name());
        return buildVertexElement(vertex);
    }

    /**
     * Builds a VertexElement from an already existing Vertex.
     * *
     *
     * @param vertex A vertex which can possibly be turned into a VertexElement
     * @return A VertexElement of
     * @throws TransactionException if vertex is not valid. A vertex is not valid if it is null or has been deleted
     */
    public VertexElement buildVertexElement(Vertex vertex) {
        if (!ElementUtils.isValidElement(vertex)) {
            Objects.requireNonNull(vertex);
            throw TransactionException.invalidElement(vertex);
        }
        return new VertexElement(this, vertex);
    }


    private GraphTraversalSource getTinkerTraversal() {
        if (graphTraversalSource == null) {
            graphTraversalSource = janusTx.traversal().withStrategies(ReadOnlyStrategy.instance());
        }
        return graphTraversalSource;
    }
}
