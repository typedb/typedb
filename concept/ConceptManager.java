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
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
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
import com.vaticle.typedb.core.concept.value.impl.ValueImpl;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SCHEMA_VALIDATION_EXCEPTIONS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_SCOPE_IS_NOT_RELATION_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ILLEGAL_ROLE_TYPE_ALIAS;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.common.parameters.Concept.Existence.STORED;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.EXPLICIT;
import static com.vaticle.typedb.core.common.parameters.Concept.Transitivity.TRANSITIVE;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.OBJECT;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.RELATION;
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
            else if (vertex.isType() && vertex.asType().isRoleType()) map.put(id, convertRoleType(vertex.asType()));
            else if (vertex.isType()) map.put(id, convertThingType(vertex.asType()));
            else if (vertex.isValue()) map.put(id, ValueImpl.of(this, vertex.asValue()));
            else throw exception(TypeDBException.of(ILLEGAL_STATE));
        });
        return map;
    }

    public GraphManager graph() {
        return graphMgr;
    }

    public ThingType getRootThingType() {
        TypeVertex vertex = graphMgr.schema().rootThingType();
        assert vertex != null;
        return convertRootThingType(vertex);
    }

    private ThingType convertRootThingType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.THING_TYPE;
        if (schemaCache == null) return new ThingTypeImpl.Root(this, vertex);
        else {
            return schemaCache.computeIfAbsent(vertex, v -> new ThingTypeImpl.Root(this, v)).asThingType();
        }
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graphMgr.schema().rootEntityType();
        assert vertex != null;
        return convertEntityType(vertex);
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graphMgr.schema().rootRelationType();
        assert vertex != null;
        return convertRelationType(vertex);
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graphMgr.schema().rootAttributeType();
        assert vertex != null;
        return convertAttributeType(vertex);
    }

    public EntityType putEntityType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return new EntityTypeImpl(this, vertex);
        else return EntityTypeImpl.of(this, label);
    }

    public EntityType getEntityType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null && vertex.encoding().equals(Encoding.Vertex.Type.ENTITY_TYPE)) {
            return convertEntityType(vertex);
        } else return null;
    }

    public RelationType putRelationType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return new RelationTypeImpl(this, vertex);
        else return RelationTypeImpl.of(this, label);
    }

    public RelationType getRelationType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null && vertex.encoding().equals(Encoding.Vertex.Type.RELATION_TYPE)) {
            return convertRelationType(vertex);
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
                if (vertex != null) return new AttributeTypeImpl.Boolean(this, vertex);
                else return new AttributeTypeImpl.Boolean(this, label);
            case LONG:
                if (vertex != null) return new AttributeTypeImpl.Long(this, vertex);
                else return new AttributeTypeImpl.Long(this, label);
            case DOUBLE:
                if (vertex != null) return new AttributeTypeImpl.Double(this, vertex);
                else return new AttributeTypeImpl.Double(this, label);
            case STRING:
                if (vertex != null) return new AttributeTypeImpl.String(this, vertex);
                else return new AttributeTypeImpl.String(this, label);
            case DATETIME:
                if (vertex != null) return new AttributeTypeImpl.DateTime(this, vertex);
                else return new AttributeTypeImpl.DateTime(this, label);
            default:
                throw exception(TypeDBException.of(UNSUPPORTED_OPERATION, "putAttributeType", valueType.name()));
        }
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null && vertex.encoding().equals(Encoding.Vertex.Type.ATTRIBUTE_TYPE)) {
            return convertAttributeType(vertex);
        } else return null;
    }

    public Type getType(Label label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) {
            if (label.scope().isPresent()) return convertRoleType(vertex);
            else return convertThingType(vertex);
        } else return null;
    }

    public ThingType getThingType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null && !vertex.encoding().equals(Encoding.Vertex.Type.ROLE_TYPE)) {
            return convertThingType(vertex);
        } else return null;
    }

    public Thing getThing(ByteArray iid) {
        ThingVertex thingVertex = graphMgr.data().getReadable(VertexIID.Thing.of(iid), true);
        if (thingVertex != null) return ThingImpl.of(this, thingVertex);
        else return null;
    }

    public Entity getEntity(ByteArray iid) {
        Thing thing = getThing(iid);
        if (thing != null) return thing.asEntity();
        else return null;
    }

    public Relation getRelation(ByteArray iid) {
        Thing thing = getThing(iid);
        if (thing != null) return thing.asRelation();
        else return null;
    }

    public Attribute getAttribute(ByteArray iid) {
        Thing thing = getThing(iid);
        if (thing != null) return thing.asAttribute();
        else return null;
    }

    public ThingType convertThingType(TypeVertex vertex) {
        assert vertex.encoding() != Encoding.Vertex.Type.ROLE_TYPE;
        return thingTypeConverter(vertex.encoding()).apply(vertex);
    }

    private Function<TypeVertex, ThingType> thingTypeConverter(Encoding.Vertex.Type encoding) {
        switch (encoding) {
            case THING_TYPE:
                return this::convertRootThingType;
            case ENTITY_TYPE:
                return this::convertEntityType;
            case ATTRIBUTE_TYPE:
                return this::convertAttributeType;
            case RELATION_TYPE:
                return this::convertRelationType;
            default:
                throw exception(TypeDBException.of(UNRECOGNISED_VALUE));
        }
    }

    public EntityType convertEntityType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.ENTITY_TYPE;
        if (schemaCache == null) return entityTypeConstructor(vertex.properLabel()).apply(this, vertex);
        else {
            return schemaCache.computeIfAbsent(vertex, v -> entityTypeConstructor(vertex.properLabel()).apply(this, v)).asEntityType();
        }
    }

    private BiFunction<ConceptManager, TypeVertex, EntityType> entityTypeConstructor(Label label) {
        if (label.equals(Encoding.Vertex.Type.Root.ENTITY.properLabel())) return EntityTypeImpl.Root::new;
        else return EntityTypeImpl::new;
    }

    public RelationType convertRelationType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.RELATION_TYPE;
        if (schemaCache == null) return relationTypeConstructor(vertex.properLabel()).apply(this, vertex);
        else {
            return schemaCache.computeIfAbsent(vertex, v -> relationTypeConstructor(vertex.properLabel()).apply(this, v)).asRelationType();
        }
    }

    private BiFunction<ConceptManager, TypeVertex, RelationType> relationTypeConstructor(Label label) {
        if (label.equals(Encoding.Vertex.Type.Root.RELATION.properLabel())) return RelationTypeImpl.Root::new;
        else return RelationTypeImpl::new;
    }

    public RoleType convertRoleType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.ROLE_TYPE;
        if (schemaCache == null) return roleTypeConstructor(vertex.properLabel()).apply(this, vertex);
        else {
            return schemaCache.computeIfAbsent(vertex, v -> roleTypeConstructor(vertex.properLabel()).apply(this, v)).asRoleType();
        }
    }

    private BiFunction<ConceptManager, TypeVertex, RoleType> roleTypeConstructor(Label label) {
        if (label.equals(Encoding.Vertex.Type.Root.ROLE.properLabel())) {
            return RoleTypeImpl.Root::new;
        } else return RoleTypeImpl::new;
    }

    public AttributeType convertAttributeType(TypeVertex vertex) {
        assert vertex.encoding() == Encoding.Vertex.Type.ATTRIBUTE_TYPE;
        if (schemaCache == null) return attributeTypeConstructor(vertex.valueType()).apply(this, vertex);
        else {
            return schemaCache.computeIfAbsent(vertex, v -> attributeTypeConstructor(vertex.valueType()).apply(this, vertex)).asAttributeType();
        }
    }

    private BiFunction<ConceptManager, TypeVertex, AttributeType> attributeTypeConstructor(Encoding.ValueType<?> valueType) {
        if (valueType == OBJECT) return AttributeTypeImpl.Root::new;
        else if (valueType == BOOLEAN) return AttributeTypeImpl.Boolean::new;
        else if (valueType == LONG) return AttributeTypeImpl.Long::new;
        else if (valueType == DOUBLE) return AttributeTypeImpl.Double::new;
        else if (valueType == STRING) return AttributeTypeImpl.String::new;
        else if (valueType == DATETIME) return AttributeTypeImpl.DateTime::new;
        throw exception(TypeDBException.of(BAD_VALUE_TYPE, valueType));
    }

    public void validateNotRoleTypeAlias(Label label) {
        assert label.scope().isPresent();
        ThingType relationType;
        if ((relationType = getThingType(label.scope().get())) == null) {
            throw TypeDBException.of(TYPE_NOT_FOUND, label.scope().get());
        } else if (!relationType.isRelationType()) {
            throw TypeDBException.of(ROLE_TYPE_SCOPE_IS_NOT_RELATION_TYPE, label.scopedName(), label.scope().get());
        } else {
            if (relationType.asRelationType().getRelates(EXPLICIT, label.name()) == null) {
                RoleType superRole = relationType.asRelationType().getRelates(TRANSITIVE, label.name());
                if (superRole != null) {
                    throw TypeDBException.of(ILLEGAL_ROLE_TYPE_ALIAS, label.scopedName(), superRole.getLabel().scopedName());
                }
            }
        }
    }

    public void validateTypes() {
        List<TypeDBException> exceptions = getSchemaExceptions();
        if (!exceptions.isEmpty()) {
            throw exception(TypeDBException.of(
                    SCHEMA_VALIDATION_EXCEPTIONS, exceptions.stream().map(Throwable::getMessage)
                            .collect(Collectors.joining("\n"))
            ));
        }
    }

    public List<TypeDBException> getSchemaExceptions() {
        if (!graphMgr.schema().hasModifiedTypes()) return list();
        else return list(getRootThingType(), getRootRelationType().getRelates().first().get())
                .stream().flatMap(t -> t.getSubtypes().stream()).filter(t -> !t.isRoot()).parallel()
                .flatMap(t -> t.exceptions().stream()).collect(toList());
    }

    public void cleanupRelations() {
        boolean deleted = true;
        while (deleted) {
            deleted = graphMgr.data().writeVertices()
                    .filter(v -> v.existence().equals(STORED) && v.isModified() && v.encoding().equals(RELATION))
                    .map(v -> ThingImpl.of(this, v).asRelation().deleteIfNoPlayer())
                    .anyMatch(d -> d);
        }
    }

    public void validateThings() {
        List<List<Thing>> lists = graphMgr.data().writeVertices().filter(
                v -> v.existence().equals(STORED) && v.isModified() && !v.encoding().equals(ROLE)
        ).<Thing>map(v -> ThingImpl.of(this, v)).toLists(PARALLELISATION_SPLIT_MINIMUM, PARALLELISATION_FACTOR);
        assert !lists.isEmpty();
        if (lists.size() == 1) {
            for (Thing thing : lists.get(0)) thing.validate();
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
        getRootAttributeType().getSubtypes(EXPLICIT).stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(at -> at.getSyntaxRecursive(stringBuilder));
        getRootRelationType().getSubtypes(EXPLICIT).stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(rt -> rt.getSyntaxRecursive(stringBuilder));
        getRootEntityType().getSubtypes(EXPLICIT).stream().sorted(comparing(x -> x.getLabel().name()))
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
