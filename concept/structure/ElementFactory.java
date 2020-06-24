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

package grakn.core.concept.structure;

import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.core.Schema;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.GraknElementException;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;


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
    private final JanusTraversalSourceProvider traversalSourceProvider;

    public ElementFactory(JanusGraphTransaction janusTransaction, JanusTraversalSourceProvider traversalSourceProvider) {
        this.janusTx = janusTransaction;
        this.traversalSourceProvider = traversalSourceProvider;
    }

    public VertexElement getVertexWithProperty(Schema.VertexProperty key, Object value) {
        Stream<VertexElement> verticesWithProperty = getVerticesWithProperty(key, value);
        Optional<VertexElement> vertexElement = verticesWithProperty.findFirst();
        return vertexElement.orElse(null);
    }

    public Stream<VertexElement> getVerticesWithProperty(Schema.VertexProperty key, Object value) {
        Stream<Vertex> vertices = traversalSourceProvider.getTinkerTraversal().V().has(key.name(), value).toStream();
        return vertices.map(vertex -> buildVertexElement(vertex));
    }

    public Vertex getVertexWithId(String id) {
        Iterator<Vertex> vertices = traversalSourceProvider.getTinkerTraversal().V(id);
        if (vertices.hasNext()) {
            return vertices.next();
        }
        return null;
    }



    // ---------------------------------------- Non Concept Construction -----------------------------------------------
    public EdgeElement buildEdgeElement(Edge edge) {
        return new EdgeElementImpl(this, edge);
    }


    Shard createShard(VertexElement shardOwner, VertexElement newShardVertex) {
        return new ShardImpl(shardOwner, newShardVertex);
    }

    public Shard getShard(Vertex vertex) {
        return new ShardImpl(buildVertexElement(vertex));
    }

    Shard getShard(VertexElement vertexElement) {
        return new ShardImpl(vertexElement);
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
     *
     * @param vertex A vertex which can possibly be turned into a VertexElement
     * @return A VertexElement of
     * @throws GraknElementException if vertex is not valid. A vertex is not valid if it is null or has been deleted
     */
    public VertexElement buildVertexElement(Vertex vertex) {
        if (!ElementUtils.isValidElement(vertex)) {
            Objects.requireNonNull(vertex);
            throw GraknElementException.invalidElement(vertex);
        }
        return new VertexElementImpl(this, vertex);
    }


    Stream<EdgeElement> rolePlayerEdges(String vertexId, Integer typeLabelId, Set<Integer> roleTypesIds) {
        return traversalSourceProvider.getTinkerTraversal()
                .V(vertexId)
                .outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel())
                .has(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID.name(), typeLabelId)
                .has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds))
                .toStream()
                .filter(edge -> ElementUtils.isValidElement(edge))// filter out invalid or deleted edges that are cached
                .map(edge -> buildEdgeElement(edge));
    }

    Stream<VertexElement> inFromSourceId(String startId, Schema.EdgeLabel edgeLabel) {
        return traversalSourceProvider.getTinkerTraversal()
                .V(startId)
                .in(edgeLabel.getLabel())
                .toStream()
                .map(vertex -> buildVertexElement(vertex));
    }

    Stream<VertexElement> inFromSourceIdWithProperty(String startId, Schema.EdgeLabel edgeLabel,
                                                     Schema.EdgeProperty edgeProperty, Set<Integer> roleTypesIds) {
        return traversalSourceProvider.getTinkerTraversal()
                .V(startId)
                .inE(edgeLabel.getLabel())
                .has(edgeProperty.name(), org.apache.tinkerpop.gremlin.process.traversal.P.within(roleTypesIds))
                .outV()
                .toStream()
                .map(vertex -> buildVertexElement(vertex));
    }

    EdgeElement edgeBetweenVertices(String startVertexId, String endVertexId, Schema.EdgeLabel edgeLabel) {
        GraphTraversal<Vertex, Edge> traversal = traversalSourceProvider.getTinkerTraversal()
                .V(startVertexId)
                .outE(edgeLabel.getLabel()).as("edge").otherV()
                .hasId(endVertexId)
                .select("edge");

        if (traversal.hasNext()) {
            return buildEdgeElement(traversal.next());
        } else {
            return null;
        }
    }
}
