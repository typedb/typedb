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

package grakn.core.query.executor;

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
import grakn.core.query.pattern.Pattern;
import grakn.core.query.pattern.constraint.ThingConstraint;
import grakn.core.query.pattern.constraint.ValueOperation;
import grakn.core.query.pattern.variable.ThingVariable;
import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.Variable;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.common.collection.Bytes.bytesToHexString;
import static grakn.core.common.exception.ErrorMessage.ThingRead.THING_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_CONSTRAINT_TYPE_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_CONSTRAINT_UNACCEPTED;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_IID_REASSERTION;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_IID_CONFLICT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_REASSERTION;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static java.util.stream.Collectors.toSet;

public class Inserter {

    private static final String TRACE_PREFIX = "insertwriter.";
    private final Concepts conceptMgr;
    private final Context.Query context;
    private final ConceptMap existing;
    private final Map<Reference, Thing> inserted;
    private final Set<Variable> variables;

    public Inserter(Concepts conceptMgr, List<graql.lang.pattern.variable.ThingVariable<?>> variables, Context.Query context) {
        this(conceptMgr, variables, new ConceptMap(), context);
    }

    public Inserter(Concepts conceptMgr, List<graql.lang.pattern.variable.ThingVariable<?>> variables, ConceptMap existing, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "constructor")) {
            this.conceptMgr = conceptMgr;
            this.variables = Pattern.fromGraqlThings(variables);
            this.context = context;
            this.existing = existing;
            this.inserted = new HashMap<>();
        }
    }

    public ConceptMap execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "writer")) {
            variables.forEach(variable -> {
                if (variable.isThing()) insert(variable.asThing());
            });
            return new ConceptMap(inserted);
        }
    }

    private Thing insert(ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            if (!variable.reference().isAnonymous() && inserted.containsKey(variable.reference())) {
                return inserted.get(variable.reference());
            } else if (existing.contains(variable.reference()) && variable.constraints().isEmpty()) {
                return existing.get(variable.reference()).asThing();
            } else validate(variable);

            Thing thing;

            if (existing.contains(variable.reference())) thing = existing.get(variable.reference()).asThing();
            else if (variable.iid().isPresent()) thing = getThing(variable.iid().get());
            else if (variable.isa().size() == 1) thing = insertIsa(variable.isa().iterator().next(), variable);
            else if (variable.isa().size() > 1)
                throw GraknException.of(THING_ISA_TOO_MANY.message(variable.reference()));
            else throw new GraknException(THING_ISA_MISSING.message(variable.reference()));

            if (!variable.has().isEmpty()) insertHas(thing, variable.has());

            if (!variable.reference().isAnonymous()) inserted.put(variable.reference(), thing);
            return thing;
        }
    }

    private void validate(ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
            if (existing.contains(variable.reference()) && (variable.iid().isPresent() || !variable.isa().isEmpty())) {
                if (variable.iid().isPresent()) {
                    throw new GraknException(THING_IID_REASSERTION.message(variable.reference(), variable.iid().get().iid()));
                } else {
                    throw new GraknException(THING_ISA_REASSERTION.message(
                            variable.reference(), variable.isa().iterator().next().type().label().get().label())
                    );
                }
            } else if (variable.iid().isPresent() && !variable.isa().isEmpty()) {
                throw new GraknException(THING_ISA_IID_CONFLICT.message(
                        variable.iid().get(), variable.isa().iterator().next().type().label().get().label())
                );
            } else if (!variable.neq().isEmpty()) {
                throw new GraknException(THING_CONSTRAINT_UNACCEPTED.message(GraqlToken.Comparator.NEQ));
            }
        }
    }

    private Thing getThing(ThingConstraint.IID iidConstraint) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthing")) {
            Thing thing = conceptMgr.getThing(iidConstraint.iid());
            if (thing == null) throw new GraknException(THING_NOT_FOUND.message(bytesToHexString(iidConstraint.iid())));
            else return thing;
        }
    }

    private ThingType getThingType(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthingtype")) {
            if (variable.reference().isLabel()) {
                assert variable.label().isPresent();
                Type type = conceptMgr.getType(variable.label().get().label());
                if (type == null) throw new GraknException(TYPE_NOT_FOUND.message(variable.label().get().label()));
                else return type.asThingType();
            } else {
                throw new GraknException(THING_CONSTRAINT_TYPE_VARIABLE.message(variable.reference()));
            }
        }
    }

    private RoleType getRoleType(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getroletype")) {
            if (variable.reference().isLabel()) {
                assert variable.label().isPresent();
                RelationType relationType;
                RoleType roleType;
                if ((relationType = conceptMgr.getRelationType(variable.label().get().scope().get())) != null &&
                        (roleType = relationType.getRelates(variable.label().get().label())) != null) {
                    return roleType;
                } else {
                    throw new GraknException(TYPE_NOT_FOUND.message(variable.label().get().scopedLabel()));
                }
            } else {
                throw new GraknException(THING_CONSTRAINT_TYPE_VARIABLE.message(variable.reference()));
            }
        }
    }

    private Thing insertIsa(ThingConstraint.Isa isaConstraint, ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertisa")) {
            ThingType thingType = getThingType(isaConstraint.type());

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

    private Attribute insertAttribute(AttributeType attributeType, ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertattribute")) {
            ThingConstraint.Value<?> valueConstraint;
            if (variable.value().size() > 1) {
                throw GraknException.of(ATTRIBUTE_VALUE_TOO_MANY.message(variable.reference(), attributeType.getLabel()));
            } else if (!variable.value().isEmpty() &&
                    (valueConstraint = variable.value().iterator().next()).operation().isAssignment()) {
                ValueOperation.Assignment valueAssignment = valueConstraint.operation().asAssignment();
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
                throw new GraknException(ATTRIBUTE_VALUE_MISSING.message(variable.reference(), attributeType.getLabel()));
            }
        }
    }

    private Relation insertRelation(RelationType relationType, ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertrelation")) {
            if (variable.relation().size() == 1) {
                Relation relation = relationType.create();
                variable.relation().iterator().next().players().forEach(rolePlayer -> {
                    RoleType roleType;
                    Thing player = insert(rolePlayer.player());
                    Set<RoleType> inferred;
                    if (rolePlayer.roleType().isPresent()) {
                        roleType = getRoleType(rolePlayer.roleType().get());
                    } else if ((inferred = player.getType().getPlays()
                            .filter(rt -> rt.getRelation().equals(relationType))
                            .collect(toSet())).size() == 1) {
                        roleType = inferred.iterator().next();
                    } else if (inferred.size() > 1) {
                        throw new GraknException(ROLE_TYPE_AMBIGUOUS.message(rolePlayer.player().reference()));
                    } else {
                        throw new GraknException(ROLE_TYPE_MISSING.message(rolePlayer.player().reference()));
                    }

                    relation.addPlayer(roleType, player);
                });
                return relation;
            } else if (variable.relation().size() > 1) {
                throw new GraknException(RELATION_CONSTRAINT_TOO_MANY.message(variable.reference()));
            } else { // variable.relation().isEmpty()
                throw new GraknException(RELATION_CONSTRAINT_MISSING.message(variable.reference()));
            }
        }
    }

    private void insertHas(Thing thing, Set<ThingConstraint.Has> hasConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "inserthas")) {
            hasConstraints.forEach(has -> {
                AttributeType attributeType = getThingType(has.type()).asAttributeType();
                Attribute attribute = insert(has.attribute()).asAttribute();
                if (!attributeType.equals(attribute.getType()) &&
                        attribute.getType().getSupertypes().noneMatch(sup -> sup.equals(attributeType))) {
                    throw new GraknException(ErrorMessage.ThingWrite.ATTRIBUTE_TYPE_MISMATCH.message(
                            attribute.getType().getLabel(), attributeType.getLabel()
                    ));
                }
                thing.setHas(attribute);
            });
        }
    }
}
