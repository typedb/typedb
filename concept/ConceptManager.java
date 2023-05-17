/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.concept;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.type.impl.AttributeTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.EntityTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.RelationTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.RoleTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typedb.core.concurrent.producer.ProducerIterator;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.VertexMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.ROLE;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public final class ConceptManager {

    private static final int PARALLELISATION_SPLIT_MINIMUM = 128;

    private final GraphManager graphMgr;
    private final ConcurrentMap<TypeVertex, Type> schemaCache;

    public ConceptManager(GraphManager graphMgr) {
        this.graphMgr = graphMgr;
        if (graphMgr.schema().isReadOnly()) schemaCache = new ConcurrentHashMap<>();
        else schemaCache = null;
    }

    public ConceptMap conceptMap(VertexMap vertexMap) {
        return new ConceptMap(toConcepts(vertexMap));
    }

    public ConceptMap.Sortable conceptMapOrdered(VertexMap vertexMap, ConceptMap.Sortable.Comparator comparator) {
        return new ConceptMap.Sortable(toConcepts(vertexMap), comparator);
    }

    private Map<Retrievable, Concept> toConcepts(VertexMap vertexMap) {
        Map<Retrievable, Concept> map = new HashMap<>();
        vertexMap.forEach((id, vertex) -> {
            if (vertex.isThing()) map.put(id, ThingImpl.of(this, vertex.asThing()));
            else if (vertex.isType()) map.put(id, convertType(vertex.asType()));
            else throw exception(TypeDBException.of(ILLEGAL_STATE));
        });
        return map;
    }

    public GraphManager graph() {
        return graphMgr;
    }

    public ThingType getRootThingType() {
        TypeVertex vertex = graphMgr.schema().rootThingType();
        if (vertex != null) {
            if (schemaCache == null) return new ThingTypeImpl.Root(this, vertex);
            else {
                return schemaCache.computeIfAbsent(vertex, v -> new ThingTypeImpl.Root(this, v)).asThingType();
            }
        } else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graphMgr.schema().rootEntityType();
        if (vertex != null) {
            if (schemaCache == null) return EntityTypeImpl.of(this, vertex);
            else {
                return schemaCache.computeIfAbsent(vertex, v -> EntityTypeImpl.of(this, v)).asEntityType();
            }
        } else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graphMgr.schema().rootRelationType();
        if (vertex != null) {
            if (schemaCache == null) return RelationTypeImpl.of(this, vertex);
            else {
                return schemaCache.computeIfAbsent(vertex, v -> RelationTypeImpl.of(this, v)).asRelationType();
            }
        } else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graphMgr.schema().rootAttributeType();
        if (vertex != null) {
            if (schemaCache == null) return AttributeTypeImpl.of(this, vertex);
            else {
                return schemaCache.computeIfAbsent(vertex, v -> AttributeTypeImpl.of(this, v)).asAttributeType();
            }
        } else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public EntityType putEntityType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(this, vertex);
        else return EntityTypeImpl.of(this, label);
    }

    public EntityType getEntityType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) {
            if (schemaCache == null) return EntityTypeImpl.of(this, vertex);
            else return schemaCache.computeIfAbsent(vertex, v -> EntityTypeImpl.of(this, v)).asEntityType();
        } else return null;
    }

    public RelationType putRelationType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(this, vertex);
        else return RelationTypeImpl.of(this, label);
    }

    public RelationType getRelationType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) {
            if (schemaCache == null) return RelationTypeImpl.of(this, vertex);
            else {
                return schemaCache.computeIfAbsent(vertex, v -> RelationTypeImpl.of(this, v)).asRelationType();
            }
        } else return null;
    }

    public AttributeType putAttributeType(String label, AttributeType.ValueType valueType) {
        if (valueType == null) throw exception(TypeDBException.of(ATTRIBUTE_VALUE_TYPE_MISSING, label));
        if (!valueType.isWritable()) {
            throw exception(TypeDBException.of(UNSUPPORTED_OPERATION, "putAttributeType", valueType.name()));
        }

        TypeVertex vertex = graphMgr.schema().getType(label);
        switch (valueType) {
            case BOOLEAN:
                if (vertex != null) return AttributeTypeImpl.Boolean.of(this, vertex);
                else return new AttributeTypeImpl.Boolean(this, label);
            case LONG:
                if (vertex != null) return AttributeTypeImpl.Long.of(this, vertex);
                else return new AttributeTypeImpl.Long(this, label);
            case DOUBLE:
                if (vertex != null) return AttributeTypeImpl.Double.of(this, vertex);
                else return new AttributeTypeImpl.Double(this, label);
            case STRING:
                if (vertex != null) return AttributeTypeImpl.String.of(this, vertex);
                else return new AttributeTypeImpl.String(this, label);
            case DATETIME:
                if (vertex != null) return AttributeTypeImpl.DateTime.of(this, vertex);
                else return new AttributeTypeImpl.DateTime(this, label);
            default:
                throw exception(TypeDBException.of(UNSUPPORTED_OPERATION, "putAttributeType", valueType.name()));
        }
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) {
            if (schemaCache == null) return AttributeTypeImpl.of(this, vertex);
            else return schemaCache.computeIfAbsent(vertex, v -> AttributeTypeImpl.of(this, v)).asAttributeType();
        } else return null;
    }

    public ThingType getThingType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) {
            if (schemaCache == null) return ThingTypeImpl.of(this, vertex);
            else return schemaCache.computeIfAbsent(vertex, v -> ThingTypeImpl.of(this, vertex)).asThingType();
        } else return null;
    }

    public Thing getThing(ByteArray iid) {
        ThingVertex thingVertex = graphMgr.data().getReadable(VertexIID.Thing.of(iid));
        if (thingVertex != null) return ThingImpl.of(this, thingVertex);
        else return null;
    }

    public Type convertType(TypeVertex vertex) {
        switch (vertex.encoding()) {
            case ROLE_TYPE:
                return convertRoleType(vertex);
            default:
                return convertThingType(vertex);
        }
    }

    public ThingType convertThingType(TypeVertex vertex) {
        assert vertex.encoding() != Encoding.Vertex.Type.ROLE_TYPE;
        if (schemaCache == null) return ThingTypeImpl.of(this, vertex);
        else return schemaCache.computeIfAbsent(vertex, v -> ThingTypeImpl.of(this, v)).asThingType();
    }

    public EntityType convertEntityType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.ENTITY_TYPE;
        if (schemaCache == null) return EntityTypeImpl.of(this, vertex);
        else return schemaCache.computeIfAbsent(vertex, v -> EntityTypeImpl.of(this, v)).asEntityType();
    }

    public RelationType convertRelationType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.RELATION_TYPE;
        if (schemaCache == null) return RelationTypeImpl.of(this, vertex);
        else return schemaCache.computeIfAbsent(vertex, v -> RelationTypeImpl.of(this, v)).asRelationType();
    }

    public RoleType convertRoleType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.ROLE_TYPE;
        if (schemaCache == null) return RoleTypeImpl.of(this, vertex);
        else return schemaCache.computeIfAbsent(vertex, v -> RoleTypeImpl.of(this, v)).asRoleType();
    }

    public AttributeType convertAttributeType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.ATTRIBUTE_TYPE;
        if (schemaCache == null) return AttributeTypeImpl.of(this, vertex);
        else return schemaCache.computeIfAbsent(vertex, v -> AttributeTypeImpl.of(this, v)).asAttributeType();
    }

    public void validateTypes() {
        List<TypeDBException> exceptions = getSchemaExceptions();
        if (!exceptions.isEmpty()) throw exception(TypeDBException.of(exceptions));
    }

    public List<TypeDBException> getSchemaExceptions() {
        if (!graphMgr.schema().hasModifiedTypes()) return list();
        else return list(getRootThingType(), getRootRelationType().getRelates().first().get())
                .stream().flatMap(t -> t.getSubtypes().stream()).filter(t -> !t.isRoot()).parallel()
                .flatMap(t -> t.exceptions().stream()).collect(toList());
    }

    public void validateThings() {
        List<List<Thing>> lists = graphMgr.data().vertices().filter(
                v -> !v.isInferred() && v.isModified() && !v.encoding().equals(ROLE)
        ).<Thing>map(v -> ThingImpl.of(this, v)).toLists(PARALLELISATION_SPLIT_MINIMUM, PARALLELISATION_FACTOR);
        assert !lists.isEmpty();
        if (lists.size() == 1) {
            iterate(lists.get(0)).forEachRemaining(Thing::validate);
        } else {
            ProducerIterator<Void> validationIterator = produce(async(iterate(lists).map(
                    list -> iterate(list).map(t -> {
                        t.validate();
                        return null;
                    })
            ), PARALLELISATION_FACTOR), Either.first(EXHAUSTIVE), async1());
            while (validationIterator.hasNext()) validationIterator.next();
        }
    }

    public String typesSyntax() {
        StringBuilder stringBuilder = new StringBuilder();
        getRootAttributeType().getSubtypesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(at -> at.getSyntaxRecursive(stringBuilder));
        getRootRelationType().getSubtypesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(rt -> rt.getSyntaxRecursive(stringBuilder));
        getRootEntityType().getSubtypesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(et -> et.getSyntaxRecursive(stringBuilder));
        return stringBuilder.toString();
    }

    public TypeDBException exception(ErrorMessage error) {
        return graphMgr.exception(error);
    }

    public TypeDBException exception(Exception exception) {
        return graphMgr.exception(exception);
    }
}
