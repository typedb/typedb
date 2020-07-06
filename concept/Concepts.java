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

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.thing.impl.ThingImpl;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.RelationType;
import hypergraph.concept.type.ThingType;
import hypergraph.concept.type.impl.AttributeTypeImpl;
import hypergraph.concept.type.impl.EntityTypeImpl;
import hypergraph.concept.type.impl.RelationTypeImpl;
import hypergraph.concept.type.impl.ThingTypeImpl;
import hypergraph.concept.type.impl.TypeImpl;
import hypergraph.graph.Graphs;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static hypergraph.common.exception.Error.Transaction.UNSUPPORTED_OPERATION;

public final class Concepts {

    private final Graphs graph;

    public Concepts(Graphs graph) {
        this.graph = graph;
    }

    public ThingType getRootType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.THING.label());
        if (vertex != null) return new ThingTypeImpl.Root(vertex);
        else throw new HypergraphException(Error.Internal.ILLEGAL_STATE);
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.ENTITY.label());
        if (vertex != null) return EntityTypeImpl.of(vertex);
        else throw new HypergraphException(Error.Internal.ILLEGAL_STATE);
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.RELATION.label());
        if (vertex != null) return RelationTypeImpl.of(vertex);
        else throw new HypergraphException(Error.Internal.ILLEGAL_STATE);
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graph.type().get(Schema.Vertex.Type.Root.ATTRIBUTE.label());
        if (vertex != null) return AttributeTypeImpl.of(vertex);
        else throw new HypergraphException(Error.Internal.ILLEGAL_STATE);
    }

    public EntityType putEntityType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return EntityTypeImpl.of(vertex);
        else return EntityTypeImpl.of(graph.type(), label);
    }

    public EntityType getEntityType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return EntityTypeImpl.of(vertex);
        else return null;
    }

    public RelationType putRelationType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return RelationTypeImpl.of(vertex);
        else return RelationTypeImpl.of(graph.type(), label);
    }

    public RelationType getRelationType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return RelationTypeImpl.of(vertex);
        else return null;
    }

    public AttributeType putAttributeType(String label, Class<?> valueType) {
        Schema.ValueType schema = Schema.ValueType.of(valueType);
        TypeVertex vertex = graph.type().get(label);
        switch (schema) {
            case BOOLEAN:
                if (vertex != null) return AttributeTypeImpl.Boolean.of(vertex);
                else return new AttributeTypeImpl.Boolean(graph.type(), label);
            case LONG:
                if (vertex != null) return AttributeTypeImpl.Long.of(vertex);
                else return new AttributeTypeImpl.Long(graph.type(), label);
            case DOUBLE:
                if (vertex != null) return AttributeTypeImpl.Double.of(vertex);
                else return new AttributeTypeImpl.Double(graph.type(), label);
            case STRING:
                if (vertex != null) return AttributeTypeImpl.String.of(vertex);
                else return new AttributeTypeImpl.String(graph.type(), label);
            case DATETIME:
                if (vertex != null) return AttributeTypeImpl.DateTime.of(vertex);
                else return new AttributeTypeImpl.DateTime(graph.type(), label);
            default:
                throw new HypergraphException(UNSUPPORTED_OPERATION.format("putAttributeType", valueType.getSimpleName()));
        }
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graph.type().get(label);
        if (vertex != null) return AttributeTypeImpl.of(vertex);
        else return null;
    }

    public void validateTypes() {
        List<HypergraphException> exceptions = graph.type().vertices().parallel()
                .filter(Vertex::isModified)
                .map(v -> TypeImpl.of(v).validate())
                .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
        if (!exceptions.isEmpty()) throw new HypergraphException(exceptions);
    }

    public void validateThings() {
        graph.thing().vertices().parallel()
                .filter(v -> !v.isInferred() && v.isModified() && !v.schema().equals(Schema.Vertex.Thing.ROLE))
                .forEach(v -> ThingImpl.of(v).validate());
    }
}
