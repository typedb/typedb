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

import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.ThingType;
import hypergraph.graph.Graph;
import hypergraph.graph.Schema;
import hypergraph.graph.vertex.TypeVertex;

import static hypergraph.common.exception.Error.TypeDefinition.INVALID_VALUE_CLASS;

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

    public AttributeType.Object getRootAttributeType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        if (vertex != null) return new AttributeType.Object(vertex);
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

    public AttributeType putAttributeType(String label, Class<?> valueClass) {
        Schema.ValueClass schema = Schema.ValueClass.of(valueClass);
        switch (schema) {
            case BOOLEAN:
                return putAttributeTypeBoolean(label);
            case LONG:
                return putAttributeTypeLong(label);
            case DOUBLE:
                return putAttributeTypeDouble(label);
            case STRING:
                return putAttributeTypeString(label);
            case DATETIME:
                return putAttributeTypeDateTime(label);
            default:
                throw new HypergraphException(INVALID_VALUE_CLASS.format(valueClass.getCanonicalName()));
        }
    }

    public AttributeType.Boolean putAttributeTypeBoolean(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.Boolean.of(vertex);
        else return new AttributeType.Boolean(graph.type(), label);
    }

    public AttributeType.Long putAttributeTypeLong(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.Long.of(vertex);
        else return new AttributeType.Long(graph.type(), label);
    }

    public AttributeType.Double putAttributeTypeDouble(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.Double.of(vertex);
        else return new AttributeType.Double(graph.type(), label);
    }

    public AttributeType.String putAttributeTypeString(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.String.of(vertex);
        else return new AttributeType.String(graph.type(), label);
    }

    public AttributeType.DateTime putAttributeTypeDateTime(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.DateTime.of(vertex);
        else return new AttributeType.DateTime(graph.type(), label);
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeType.of(vertex);
        else return null;
    }
}
