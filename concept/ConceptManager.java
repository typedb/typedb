/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.concept;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.thing.impl.ThingImpl;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.impl.AttributeTypeImpl;
import grakn.core.concept.type.impl.EntityTypeImpl;
import grakn.core.concept.type.impl.RelationTypeImpl;
import grakn.core.concept.type.impl.ThingTypeImpl;
import grakn.core.concept.type.impl.TypeImpl;
import grakn.core.graph.GraphManager;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.common.VertexMap;
import graql.lang.pattern.variable.Reference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Transaction.UNSUPPORTED_OPERATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;

public final class ConceptManager {

    private final GraphManager graphMgr;

    public ConceptManager(GraphManager graphMgr) {
        this.graphMgr = graphMgr;
    }

    public ResourceIterator<ConceptMap> conceptMaps(ResourceIterator<VertexMap> vertexMap) {
        return vertexMap.map(this::conceptMap);
    }

    public ConceptMap conceptMap(VertexMap vertexMap) {
        Map<Reference.Name, Concept> map = new HashMap<>();
        vertexMap.forEach((reference, vertex) -> {
            if (!reference.isName()) throw graphMgr.exception(GraknException.of(ILLEGAL_STATE));
            if (vertex.isThing()) map.put(reference.asName(), ThingImpl.of(vertex.asThing()));
            else if (vertex.isType()) map.put(reference.asName(), TypeImpl.of(graphMgr, vertex.asType()));
            else graphMgr.exception(GraknException.of(ILLEGAL_STATE));
        });
        return new ConceptMap(map);
    }

    public ThingType getRootThingType() {
        final TypeVertex vertex = graphMgr.schema().rootThingType();
        if (vertex != null) return new ThingTypeImpl.Root(graphMgr, vertex);
        else throw graphMgr.exception(GraknException.of(ILLEGAL_STATE));
    }

    public EntityType getRootEntityType() {
        final TypeVertex vertex = graphMgr.schema().rootEntityType();
        if (vertex != null) return EntityTypeImpl.of(graphMgr, vertex);
        else throw graphMgr.exception(GraknException.of(ILLEGAL_STATE));
    }

    public RelationType getRootRelationType() {
        final TypeVertex vertex = graphMgr.schema().rootRelationType();
        if (vertex != null) return RelationTypeImpl.of(graphMgr, vertex);
        else throw graphMgr.exception(GraknException.of(ILLEGAL_STATE));
    }

    public AttributeType getRootAttributeType() {
        final TypeVertex vertex = graphMgr.schema().rootAttributeType();
        if (vertex != null) return AttributeTypeImpl.of(graphMgr, vertex);
        else throw graphMgr.exception(GraknException.of(ILLEGAL_STATE));
    }

    public EntityType putEntityType(String label) {
        final TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(graphMgr, vertex);
        else return EntityTypeImpl.of(graphMgr, label);
    }

    public EntityType getEntityType(String label) {
        final TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public RelationType putRelationType(String label) {
        final TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(graphMgr, vertex);
        else return RelationTypeImpl.of(graphMgr, label);
    }

    public RelationType getRelationType(String label) {
        final TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public AttributeType putAttributeType(String label, AttributeType.ValueType valueType) {
        if (valueType == null) throw graphMgr.exception(GraknException.of(ATTRIBUTE_VALUE_TYPE_MISSING, label));
        if (!valueType.isWritable()) throw graphMgr.exception(GraknException.of(UNSUPPORTED_OPERATION));

        final TypeVertex vertex = graphMgr.schema().getType(label);
        switch (valueType) {
            case BOOLEAN:
                if (vertex != null) return AttributeTypeImpl.Boolean.of(graphMgr, vertex);
                else return new AttributeTypeImpl.Boolean(graphMgr, label);
            case LONG:
                if (vertex != null) return AttributeTypeImpl.Long.of(graphMgr, vertex);
                else return new AttributeTypeImpl.Long(graphMgr, label);
            case DOUBLE:
                if (vertex != null) return AttributeTypeImpl.Double.of(graphMgr, vertex);
                else return new AttributeTypeImpl.Double(graphMgr, label);
            case STRING:
                if (vertex != null) return AttributeTypeImpl.String.of(graphMgr, vertex);
                else return new AttributeTypeImpl.String(graphMgr, label);
            case DATETIME:
                if (vertex != null) return AttributeTypeImpl.DateTime.of(graphMgr, vertex);
                else return new AttributeTypeImpl.DateTime(graphMgr, label);
            default:
                throw graphMgr.exception(GraknException.of(UNSUPPORTED_OPERATION, "putAttributeType", valueType.name()));
        }
    }

    public AttributeType getAttributeType(String label) {
        final TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return AttributeTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public ThingType getThingType(String label) {
        final TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return ThingTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public Thing getThing(byte[] iid) {
        final ThingVertex thingVertex = graphMgr.data().get(VertexIID.Thing.of(iid));
        if (thingVertex != null) return ThingImpl.of(thingVertex);
        else return null;
    }

    public void validateTypes() {
        final List<GraknException> exceptions = graphMgr.schema().bufferedTypes().parallel()
                .filter(Vertex::isModified)
                .map(v -> TypeImpl.of(graphMgr, v).validate())
                .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
        if (!exceptions.isEmpty()) throw graphMgr.exception(GraknException.of(exceptions));
    }

    public void validateThings() {
        graphMgr.data().vertices().parallel()
                .filter(v -> !v.isInferred() && v.isModified() && !v.encoding().equals(Encoding.Vertex.Thing.ROLE))
                .forEach(v -> ThingImpl.of(v).validate());
    }

    public GraknException exception(ErrorMessage error) {
        return graphMgr.exception(error);
    }

    public GraknException exception(Exception exception) {
        return graphMgr.exception(exception);
    }
}
