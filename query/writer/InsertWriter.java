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

package grakn.core.query.writer;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.concept.Concepts;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.property.ThingProperty;
import graql.lang.pattern.property.ValueOperation;
import graql.lang.pattern.variable.BoundVariable;
import graql.lang.pattern.variable.Identity;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.TypeVariable;
import graql.lang.query.GraqlInsert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Bytes.hexStringToBytes;
import static grakn.core.common.exception.ErrorMessage.ThingRead.THING_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_IID_REASSERTION;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_IID_CONFLICT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_REASSERTION;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_PROPERTY_TYPE_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_PROPERTY_UNACCEPTED;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static java.util.stream.Collectors.toSet;

public class InsertWriter {

    private static final String TRACE_PREFIX = "insertwriter.";
    private final Concepts conceptMgr;
    private final Context.Query context;
    private final ConceptMap existing;
    private final Map<Identity, Thing> inserted;
    private Map<Identity, BoundVariable<?>> variables;

    public InsertWriter(Concepts conceptMgr, GraqlInsert query, Context.Query context) {
        this(conceptMgr, query, context, new ConceptMap());
    }

    public InsertWriter(Concepts conceptMgr, GraqlInsert query, Context.Query context, ConceptMap existing) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "constructor")) {
            this.conceptMgr = conceptMgr;
            this.context = context;
            this.existing = existing;
            this.variables = query.asGraph();
            this.inserted = new HashMap<>();
        }
    }

    public ConceptMap write() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "writer")) {
            this.variables.forEach((identity, boundVariable) -> {
                if (boundVariable.isThing()) insert(identity);
            });
            return new ConceptMap(inserted);
        }
    }

    private Thing insert(Identity identity) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            if (inserted.containsKey(identity)) return inserted.get(identity);
            else if (existing.contains(identity) && !variables.get(identity).properties().isEmpty()) {
                return existing.get(identity).asThing();
            } else validate(identity);

            ThingVariable<?> variable = variables.get(identity).asThing();
            Thing thing;

            if (existing.contains(identity)) thing = existing.get(identity).asThing();
            else if (variable.iid().isPresent()) thing = getThing(variable.iid().get());
            else if (variable.isa().isPresent()) thing = insertIsa(variable.isa().get(), variable);
            else throw new GraknException(THING_ISA_MISSING.message(identity.toString()));

            if (!variable.has().isEmpty()) insertHas(thing, variable.has(), identity);

            inserted.put(identity, thing);
            return thing;
        }
    }

    private void validate(Identity identity) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
            ThingVariable<?> variable = variables.get(identity).asThing();
            if (existing.contains(identity) && (variable.iid().isPresent() || variable.isa().isPresent())) {
                if (variable.iid().isPresent()) {
                    throw new GraknException(THING_IID_REASSERTION.message(identity, variable.iid().get().iid()));
                } else {
                    throw new GraknException(THING_ISA_REASSERTION.message(
                            identity, variable.isa().get().type().label().get().label())
                    );
                }
            } else if (variable.iid().isPresent() && variable.isa().isPresent()) {
                throw new GraknException(THING_ISA_IID_CONFLICT.message(
                        variable.iid().get(), variable.isa().get().type().label().get().label())
                );
            } else if (variable.neq().isPresent()) {
                throw new GraknException(THING_PROPERTY_UNACCEPTED.message(GraqlToken.Comparator.NEQ));
            }
        }
    }

    private Thing getThing(ThingProperty.IID iidProperty) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthing")) {
            Thing thing = conceptMgr.getThing(hexStringToBytes(iidProperty.iid()));
            if (thing == null) throw new GraknException(THING_NOT_FOUND.message(iidProperty.iid()));
            else return thing;
        }
    }

    private ThingType getThingType(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthingtype")) {
            if (variable.isLabelled()) {
                assert variable.label().isPresent();
                Type type = conceptMgr.getType(variable.label().get().label());
                if (type == null) throw new GraknException(TYPE_NOT_FOUND.message(variable.label().get().label()));
                else return type.asThingType();
            } else {
                throw new GraknException(THING_PROPERTY_TYPE_VARIABLE.message(variable.identity()));
            }
        }
    }

    private RoleType getRoleType(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getroletype")) {
            if (variable.isLabelled()) {
                assert variable.label().isPresent();
                RelationType relationType; RoleType roleType;
                if ((relationType = conceptMgr.getRelationType(variable.label().get().scope().get())) != null &&
                        (roleType = relationType.getRelates(variable.label().get().label())) != null) {
                    return roleType;
                } else {
                    throw new GraknException(TYPE_NOT_FOUND.message(variable.label().get().scopedLabel()));
                }
            } else {
                throw new GraknException(THING_PROPERTY_TYPE_VARIABLE.message(variable.identity()));
            }
        }
    }

    private Thing insertIsa(ThingProperty.Isa isaProperty, ThingVariable<?> variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertisa")) {
            ThingType thingType = getThingType(isaProperty.type());

            if (thingType instanceof EntityType) {
                return insertEntity(thingType.asEntityType());
            } else if (thingType instanceof AttributeType) {
                return insertAttribute(thingType.asAttributeType(), variable);
            } else if (thingType instanceof RelationType) {
                return insertRelation(thingType.asRelationType(), variable);
            } else {
                assert false;
                return null;
            }
        }
    }

    private Entity insertEntity(EntityType entityType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertentity")) {
            return entityType.create();
        }
    }

    private Attribute insertAttribute(AttributeType attributeType, ThingVariable<?> variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertattribute")) {
            ThingProperty.Value<?> valueProperty;
            if (variable.value().isPresent() && (valueProperty = variable.value().get()).operation().isAssignment()) {
                ValueOperation.Assignment valueAssignment = valueProperty.operation().asAssignment();
                switch (attributeType.getValueType()) {
                    case LONG:
                        return attributeType.asLong().put(valueAssignment.asLong().value());
                    case DOUBLE:
                        return attributeType.asDouble().put(valueAssignment.asDouble().value());
                    case BOOLEAN:
                        return attributeType.asBoolean().put(valueAssignment.asBoolean().value());
                    case STRING:
                        return attributeType.asString().put(valueAssignment.asString().value());
                    case DATETIME:
                        return attributeType.asDateTime().put(valueAssignment.asDateTime().value());
                    default:
                        assert false;
                        return null;
                }
            } else {
                throw new GraknException(ATTRIBUTE_VALUE_MISSING.message(variable.identity(), attributeType.getLabel()));
            }
        }
    }

    private Relation insertRelation(RelationType relationType, ThingVariable<?> variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "inserrelation")) {
            if (variable.relation().isPresent()) {
                Relation relation = relationType.create();
                variable.relation().get().players().forEach(rolePlayer -> {
                    RoleType roleType;
                    Thing player = insert(rolePlayer.player().identity());
                    Set<RoleType> inferred;
                    if (rolePlayer.roleType().isPresent()) {
                        roleType = getRoleType(rolePlayer.roleType().get());
                    } else if ((inferred = player.getType().getPlays()
                            .filter(rt -> rt.getRelation().equals(relationType))
                            .collect(toSet())).size() == 1) {
                        roleType = inferred.iterator().next();
                    } else if (inferred.size() > 1) {
                        throw new GraknException(ROLE_TYPE_AMBIGUOUS.message(rolePlayer.player().identity()));
                    } else {
                        throw new GraknException(ROLE_TYPE_MISSING.message(rolePlayer.player().identity()));
                    }

                    relation.addPlayer(roleType, player);
                });

                return relation;
            } else {
                throw new GraknException(ErrorMessage.ThingWrite.RELATION_PROPERTY_MISSING.message(variable.identity()));
            }
        }
    }

    private void insertHas(Thing thing, List<ThingProperty.Has> hasProperties, Identity identity) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "inserthas")) {
            hasProperties.forEach(has -> {
                AttributeType attributeType = getThingType(has.type()).asAttributeType();
                Attribute attribute = insert(has.attribute().identity()).asAttribute();
                if (!attributeType.equals(attribute.getType()) &&
                        attribute.getType().getSupertypes().noneMatch(sup -> sup.equals(attributeType))) {
                    throw new GraknException(ErrorMessage.ThingWrite.ATTRIBUTE_TYPE_MISMATCH.message(
                            identity, attribute.getType().getLabel(), attributeType.getLabel()
                    ));
                }
                thing.setHas(attribute);
            });
        }
    }
}
