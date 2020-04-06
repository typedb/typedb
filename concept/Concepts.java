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
import hypergraph.concept.type.ThingType;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

public class Concepts {

    private final Graph graph;

    public Concepts(Graph graph) {
        this.graph = graph;
    }

    public ThingType getRootType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.THING.label());
        if (vertex != null) return new ThingType.Root(vertex);
        else return null;
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.ENTITY.label());
        if (vertex != null) return EntityType.of(vertex);
        else return null;
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.RELATION.label());
        if (vertex != null) return RelationType.of(vertex);
        else return null;
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        if (vertex != null) return AttributeType.of(vertex);
        else return null;
    }

    public EntityType putEntityType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return EntityType.of(vertex);
        else return EntityType.of(graph.type(), label);
    }

    public EntityType getEntityType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return EntityType.of(vertex);
        else return null;
    }

    public RelationType putRelationType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return RelationType.of(vertex);
        else return RelationType.of(graph.type(), label);
    }

    public RelationType getRelationType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return RelationType.of(vertex);
        else return null;
    }

    public AttributeType putAttributeType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.of(vertex);
        else return AttributeType.of(graph.type(), label);
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.of(vertex);
        else return null;
    }
}
