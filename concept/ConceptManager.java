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

package hypergraph.concept;

import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.Type;
import hypergraph.graph.GraphManager;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

public class ConceptManager {

    private final GraphManager graph;

    public ConceptManager(GraphManager graph) {
        this.graph = graph;
    }

    public Type getRootType() {
        TypeVertex vertex = graph.getTypeVertex(Schema.Vertex.Type.Root.THING.label());
        if (vertex != null) return new Type(vertex);
        else return null;
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graph.getTypeVertex(Schema.Vertex.Type.Root.ENTITY.label());
        if (vertex != null) return new EntityType(vertex);
        else return null;
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graph.getTypeVertex(Schema.Vertex.Type.Root.RELATION.label());
        if (vertex != null) return new RelationType(vertex);
        else return null;
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graph.getTypeVertex(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        if (vertex != null) return new AttributeType(vertex);
        else return null;
    }

    public EntityType getEntityType(String label) {
        return null;
    }

    public RelationType getRelationType(String label) {
        return null;
    }

    public AttributeType getAttributeType(String label) {
        return null;
    }

    public EntityType putEntityType(String label) {
        return putEntityType(label, Schema.Vertex.Type.Root.ENTITY.label());
    }

    public EntityType putEntityType(String label, String parent) {
        TypeVertex entityTypeVertex = graph.getTypeVertex(label);

        if (entityTypeVertex == null) {
            entityTypeVertex = graph.createTypeVertex(Schema.Vertex.Type.ENTITY_TYPE, label);
            TypeVertex parentTypeVertex = graph.getTypeVertex(parent);
            graph.createEdge(Schema.Edge.SUB, entityTypeVertex, parentTypeVertex);
        }

        return new EntityType(entityTypeVertex);
    }
}
