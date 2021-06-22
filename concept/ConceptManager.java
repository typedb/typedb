/*
 * Copyright (C) 2021 Vaticle
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
import com.vaticle.typedb.core.common.util.StringBuilders;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.thing.impl.ThingImpl;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.impl.AttributeTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.EntityTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.RelationTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.ThingTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.TypeImpl;
import com.vaticle.typedb.core.concurrent.producer.ProducerIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.VertexMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.concept.ConceptManager.TypeExporter.writeAttributeType;
import static com.vaticle.typedb.core.concept.ConceptManager.TypeExporter.writeEntityType;
import static com.vaticle.typedb.core.concept.ConceptManager.TypeExporter.writeRelationType;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ROLE;
import static com.vaticle.typeql.lang.common.util.Strings.escapeRegex;
import static com.vaticle.typeql.lang.common.util.Strings.quoteString;
import static java.util.Comparator.comparing;

public final class ConceptManager {

    private static final int PARALLELISATION_SPLIT_MINIMUM = 128;

    private final GraphManager graphMgr;

    public ConceptManager(GraphManager graphMgr) {
        this.graphMgr = graphMgr;
    }

    public ConceptMap conceptMap(VertexMap vertexMap) {
        Map<Retrievable, Concept> map = new HashMap<>();
        vertexMap.forEach((id, vertex) -> {
            if (vertex.isThing()) map.put(id, ThingImpl.of(vertex.asThing()));
            else if (vertex.isType()) map.put(id, TypeImpl.of(graphMgr, vertex.asType()));
            else throw exception(TypeDBException.of(ILLEGAL_STATE));
        });
        return new ConceptMap(map);
    }

    public ThingType getRootThingType() {
        TypeVertex vertex = graphMgr.schema().rootThingType();
        if (vertex != null) return new ThingTypeImpl.Root(graphMgr, vertex);
        else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graphMgr.schema().rootEntityType();
        if (vertex != null) return EntityTypeImpl.of(graphMgr, vertex);
        else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graphMgr.schema().rootRelationType();
        if (vertex != null) return RelationTypeImpl.of(graphMgr, vertex);
        else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graphMgr.schema().rootAttributeType();
        if (vertex != null) return AttributeTypeImpl.of(graphMgr, vertex);
        else throw exception(TypeDBException.of(ILLEGAL_STATE));
    }

    public EntityType putEntityType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(graphMgr, vertex);
        else return EntityTypeImpl.of(graphMgr, label);
    }

    public EntityType getEntityType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public RelationType putRelationType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(graphMgr, vertex);
        else return RelationTypeImpl.of(graphMgr, label);
    }

    public RelationType getRelationType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public AttributeType putAttributeType(String label, AttributeType.ValueType valueType) {
        if (valueType == null) throw exception(TypeDBException.of(ATTRIBUTE_VALUE_TYPE_MISSING, label));
        if (!valueType.isWritable()) throw exception(TypeDBException.of(UNSUPPORTED_OPERATION));

        TypeVertex vertex = graphMgr.schema().getType(label);
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
                throw exception(TypeDBException.of(UNSUPPORTED_OPERATION, "putAttributeType", valueType.name()));
        }
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return AttributeTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public ThingType getThingType(String label) {
        TypeVertex vertex = graphMgr.schema().getType(label);
        if (vertex != null) return ThingTypeImpl.of(graphMgr, vertex);
        else return null;
    }

    public Thing getThing(ByteArray iid) {
        ThingVertex thingVertex = graphMgr.data().get(VertexIID.Thing.of(iid));
        if (thingVertex != null) return ThingImpl.of(thingVertex);
        else return null;
    }

    public void validateTypes() {
        List<TypeDBException> exceptions = graphMgr.schema().bufferedTypes().parallel()
                .filter(TypeVertex::isModified)
                .map(v -> TypeImpl.of(graphMgr, v).validate())
                .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
        if (!exceptions.isEmpty()) throw exception(TypeDBException.of(exceptions));
    }

    public void validateThings() {
        List<List<Thing>> lists = graphMgr.data().vertices().filter(
                v -> !v.isInferred() && v.isModified() && !v.encoding().equals(ROLE)
        ).<Thing>map(ThingImpl::of).toLists(PARALLELISATION_SPLIT_MINIMUM, PARALLELISATION_FACTOR);
        assert !lists.isEmpty();
        if (lists.size() == 1) {
            iterate(lists.get(0)).forEachRemaining(Thing::validate);
        } else {
            ProducerIterator<Void> validationIterator = produce(async(iterate(lists).map(
                    list -> iterate(list).map(t -> {
                        t.validate();
                        return (Void) null;
                    })
            ), PARALLELISATION_FACTOR), Either.first(EXHAUSTIVE), async1());
            while (validationIterator.hasNext()) validationIterator.next();
        }
    }

    public void exportTypes(StringBuilder stringBuilder) {
        getRootAttributeType().getSubtypesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(x -> writeAttributeType(stringBuilder, x));
        getRootRelationType().getSubtypesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(x -> writeRelationType(stringBuilder, x));
        getRootEntityType().getSubtypesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                .forEach(x -> writeEntityType(stringBuilder, x));
    }

    public TypeDBException exception(ErrorMessage error) {
        return graphMgr.exception(error);
    }

    public TypeDBException exception(Exception exception) {
        return graphMgr.exception(exception);
    }

    // TODO: This class should be dissolved and its logic should be moved to the appropriate Types
    public static class TypeExporter {

        static void writeAttributeType(StringBuilder builder, AttributeType attributeType) {
            builder.append(String.format("%s sub %s",
                                         attributeType.getLabel().name(),
                                         attributeType.getSupertype().getLabel().name()))
                    .append(StringBuilders.COMMA_NEWLINE_INDENT)
                    .append(String.format("value %s", getValueTypeString(attributeType.getValueType())));
            if (attributeType.isString()) {
                java.util.regex.Pattern regex = attributeType.asString().getRegex();
                if (regex != null) {
                    builder.append(StringBuilders.COMMA_NEWLINE_INDENT)
                            .append(String.format("regex %s", quoteString(escapeRegex(regex.pattern()))));
                }
            }
            writeAbstract(builder, attributeType);
            writeOwns(builder, attributeType);
            writePlays(builder, attributeType);
            builder.append(StringBuilders.SEMICOLON_NEWLINE_X2);
            attributeType.getSubtypesExplicit().stream()
                    .sorted(comparing(x -> x.getLabel().name()))
                    .forEach(x -> writeAttributeType(builder, x));
        }

        static void writeRelationType(StringBuilder builder, RelationType relationType) {
            builder.append(String.format("%s sub %s",
                                         relationType.getLabel().name(),
                                         relationType.getSupertype().getLabel().name()));
            writeAbstract(builder, relationType);
            writeOwns(builder, relationType);
            writeRelates(builder, relationType);
            writePlays(builder, relationType);
            builder.append(StringBuilders.SEMICOLON_NEWLINE_X2);
            relationType.getSubtypesExplicit().stream()
                    .sorted(comparing(x -> x.getLabel().name()))
                    .forEach(x -> writeRelationType(builder, x));
        }

        static void writeEntityType(StringBuilder builder, EntityType entityType) {
            builder.append(String.format("%s sub %s",
                                         entityType.getLabel().name(),
                                         entityType.getSupertype().getLabel().name()));
            writeAbstract(builder, entityType);
            writeOwns(builder, entityType);
            writePlays(builder, entityType);
            builder.append(StringBuilders.SEMICOLON_NEWLINE_X2);
            entityType.getSubtypesExplicit().stream()
                    .sorted(comparing(x -> x.getLabel().name()))
                    .forEach(x -> writeEntityType(builder, x));
        }

        private static void writeAbstract(StringBuilder builder, ThingType thingType) {
            if (thingType.isAbstract()) {
                builder.append(StringBuilders.COMMA_NEWLINE_INDENT).append("abstract");
            }
        }

        private static void writeOwns(StringBuilder builder, ThingType thingType) {
            Set<String> keys = thingType.getOwnsExplicit(true).map(x -> x.getLabel().name()).toSet();
            List<? extends AttributeType> attributeTypes = thingType.getOwnsExplicit().toList();
            attributeTypes.stream().filter(x -> keys.contains(x.getLabel().name()))
                    .sorted(comparing(x -> x.getLabel().name()))
                    .forEach(attributeType -> {
                        builder.append(StringBuilders.COMMA_NEWLINE_INDENT)
                                .append(String.format("owns %s", attributeType.getLabel().name()));
                        AttributeType overridden = thingType.getOwnsOverridden(attributeType);
                        if (overridden != null) {
                            builder.append(String.format(" as %s", overridden.getLabel().name()));
                        }
                        builder.append(" @key");
                    });
            attributeTypes.stream().filter(x -> !keys.contains(x.getLabel().name()))
                    .sorted(comparing(x -> x.getLabel().name()))
                    .forEach(attributeType -> {
                        builder.append(StringBuilders.COMMA_NEWLINE_INDENT)
                                .append(String.format("owns %s", attributeType.getLabel().name()));
                        AttributeType overridden = thingType.getOwnsOverridden(attributeType);
                        if (overridden != null) {
                            builder.append(String.format(" as %s", overridden.getLabel().name()));
                        }
                    });
        }

        private static void writeRelates(StringBuilder builder, RelationType relationType) {
            relationType.getRelatesExplicit().stream().sorted(comparing(x -> x.getLabel().name()))
                    .forEach(roleType -> {
                        builder.append(StringBuilders.COMMA_NEWLINE_INDENT)
                                .append(String.format("relates %s", roleType.getLabel().name()));
                        RoleType overridden = relationType.getRelatesOverridden(roleType.getLabel().name());
                        if (overridden != null) {
                            builder.append(String.format(" as %s", overridden.getLabel().name()));
                        }
                    });
        }

        private static void writePlays(StringBuilder builder, ThingType thingType) {
            thingType.getPlaysExplicit().stream().sorted(comparing(x -> x.getLabel().scopedName()))
                    .forEach(roleType -> {
                        builder.append(StringBuilders.COMMA_NEWLINE_INDENT)
                                .append(String.format("plays %s", roleType.getLabel().scopedName()));
                        RoleType overridden = thingType.getPlaysOverridden(roleType);
                        if (overridden != null) {
                            builder.append(String.format(" as %s", overridden.getLabel().scopedName()));
                        }
                    });
        }

        private static String getValueTypeString(AttributeType.ValueType valueType) {
            switch (valueType) {
                case STRING:
                    return "string";
                case LONG:
                    return "long";
                case DOUBLE:
                    return "double";
                case BOOLEAN:
                    return "boolean";
                case DATETIME:
                    return "datetime";
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }
}
