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

package grakn.core.query;

import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.concept.ConceptManager;
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
import grakn.core.concept.type.impl.ThingTypeImpl;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableRegistry;
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
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
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
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static graql.lang.common.GraqlToken.Constraint.IS;
import static java.util.stream.Collectors.toSet;

public class Inserter {

    private static final String TRACE_PREFIX = "inserter.";

    private final ConceptManager conceptMgr;
    private final Context.Query context;
    private final ConceptMap existing;
    private final Map<Reference, Thing> inserted;
    private final Set<ThingVariable> variables;

    private Inserter(ConceptManager conceptMgr, Set<ThingVariable> variables,
                     ConceptMap existing, Context.Query context) {
        this.conceptMgr = conceptMgr;
        this.variables = variables;
        this.context = context;
        this.existing = existing;
        this.inserted = new HashMap<>();
    }

    public static Inserter create(ConceptManager conceptMgr,
                                  List<graql.lang.pattern.variable.ThingVariable<?>> variables,
                                  Context.Query context) {
        return create(conceptMgr, variables, new ConceptMap(), context);
    }

    public static Inserter create(ConceptManager conceptMgr,
                                  List<graql.lang.pattern.variable.ThingVariable<?>> variables,
                                  ConceptMap existing, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            return new Inserter(conceptMgr, VariableRegistry.createFromThings(variables).things(), existing, context);
        }
    }

    public ConceptMap execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            variables.forEach(this::insert);
            return answer();
        }
    }

    private ConceptMap answer() {
        Map<Reference.Name, Thing> answerMap = new HashMap<>();
        inserted.forEach((ref, thing) -> {
            if (ref.isName()) answerMap.put(ref.asName(), thing);
        });
        return new ConceptMap(answerMap);
    }

    private boolean existingContains(ThingVariable variable) {
        return variable.reference().isName() && existing.contains(variable.reference().asName());
    }

    public Thing existingGet(ThingVariable variable) {
        return existing.get(variable.reference().asName()).asThing();
    }

    private Thing insert(ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            if (!variable.reference().isAnonymous() && inserted.containsKey(variable.reference())) {
                return inserted.get(variable.reference());
            } else if (existingContains(variable) && variable.constraints().isEmpty()) {
                return existingGet(variable);
            } else validate(variable);

            final Thing thing;

            if (existingContains(variable)) thing = existingGet(variable);
            else if (variable.iid().isPresent()) thing = getThing(variable.iid().get());
            else if (variable.isa().isPresent()) thing = insertIsa(variable.isa().get(), variable);
            else throw GraknException.of(THING_ISA_MISSING, variable.reference());

            if (!variable.has().isEmpty()) insertHas(thing, variable.has());

            if (!variable.reference().isAnonymous()) inserted.put(variable.reference(), thing);
            return thing;
        }
    }

    private void validate(ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
            if (existingContains(variable) && (variable.iid().isPresent() || variable.isa().isPresent())) {
                if (variable.iid().isPresent()) {
                    throw GraknException.of(THING_IID_REASSERTION, variable.reference(), variable.iid().get().iid());
                } else {
                    throw GraknException.of(THING_ISA_REASSERTION, variable.reference(),
                                            variable.isa().get().type().label().get().label());
                }
            } else if (variable.iid().isPresent() && variable.isa().isPresent()) {
                throw GraknException.of(THING_ISA_IID_CONFLICT, variable.iid().get(),
                                        variable.isa().get().type().label().get().label());
            } else if (!variable.is().isEmpty()) {
                throw GraknException.of(THING_CONSTRAINT_UNACCEPTED, IS);
            }
        }
    }

    private Thing getThing(IIDConstraint iidConstraint) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthing")) {
            final Thing thing = conceptMgr.getThing(iidConstraint.iid());
            if (thing == null) throw GraknException.of(THING_NOT_FOUND, bytesToHexString(iidConstraint.iid()));
            else return thing;
        }
    }

    private ThingType getThingType(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getthingtype")) {
            if (variable.reference().isLabel()) {
                assert variable.label().isPresent();
                final ThingType thingType = conceptMgr.getThingType(variable.label().get().label());
                if (thingType == null) throw GraknException.of(TYPE_NOT_FOUND, variable.label().get().label());
                else return thingType.asThingType();
            } else {
                throw GraknException.of(THING_CONSTRAINT_TYPE_VARIABLE, variable.reference());
            }
        }
    }

    private RoleType getRoleType(TypeVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "getroletype")) {
            if (variable.reference().isLabel()) {
                assert variable.label().isPresent();
                final RelationType relationType;
                final RoleType roleType;
                if ((relationType = conceptMgr.getRelationType(variable.label().get().scope().get())) != null &&
                        (roleType = relationType.getRelates(variable.label().get().label())) != null) {
                    return roleType;
                } else {
                    throw GraknException.of(TYPE_NOT_FOUND, variable.label().get().scopedLabel());
                }
            } else {
                throw GraknException.of(THING_CONSTRAINT_TYPE_VARIABLE, variable.reference());
            }
        }
    }

    private Thing insertIsa(IsaConstraint isaConstraint, ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertisa")) {
            final ThingType thingType = getThingType(isaConstraint.type());

            if (thingType instanceof EntityType) {
                return insertEntity(thingType.asEntityType());
            } else if (thingType instanceof AttributeType) {
                return insertAttribute(thingType.asAttributeType(), variable);
            } else if (thingType instanceof RelationType) {
                return insertRelation(thingType.asRelationType(), variable);
            } else if (thingType instanceof ThingTypeImpl.Root) {
                throw GraknException.of(ILLEGAL_ABSTRACT_WRITE, Thing.class.getSimpleName(), thingType.getLabel());
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
            final ValueConstraint<?> valueConstraint;
            if (variable.value().size() > 1) {
                throw GraknException.of(ATTRIBUTE_VALUE_TOO_MANY, variable.reference(), attributeType.getLabel());
            } else if (!variable.value().isEmpty() &&
                    (valueConstraint = variable.value().iterator().next()).isValueIdentity()) {
                switch (attributeType.getValueType()) {
                    case LONG:
                        return attributeType.asLong().put(valueConstraint.asLong().value());
                    case DOUBLE:
                        return attributeType.asDouble().put(valueConstraint.asDouble().value());
                    case BOOLEAN:
                        return attributeType.asBoolean().put(valueConstraint.asBoolean().value());
                    case STRING:
                        return attributeType.asString().put(valueConstraint.asString().value());
                    case DATETIME:
                        return attributeType.asDateTime().put(valueConstraint.asDateTime().value());
                    default:
                        assert false;
                        return null;
                }
            } else {
                throw GraknException.of(ATTRIBUTE_VALUE_MISSING, variable.reference(), attributeType.getLabel());
            }
        }
    }

    private Relation insertRelation(RelationType relationType, ThingVariable variable) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insertrelation")) {
            if (variable.relation().size() == 1) {
                final Relation relation = relationType.create();
                variable.relation().iterator().next().players().forEach(rolePlayer -> {
                    final RoleType roleType;
                    final Thing player = insert(rolePlayer.player());
                    final Set<RoleType> inferred;
                    if (rolePlayer.roleType().isPresent()) {
                        roleType = getRoleType(rolePlayer.roleType().get());
                    } else if ((inferred = player.getType().getPlays()
                            .filter(rt -> rt.getRelationType().equals(relationType))
                            .collect(toSet())).size() == 1) {
                        roleType = inferred.iterator().next();
                    } else if (inferred.size() > 1) {
                        throw GraknException.of(ROLE_TYPE_AMBIGUOUS, rolePlayer.player().reference());
                    } else {
                        throw GraknException.of(ROLE_TYPE_MISSING, rolePlayer.player().reference());
                    }

                    relation.addPlayer(roleType, player);
                });
                return relation;
            } else if (variable.relation().size() > 1) {
                throw GraknException.of(RELATION_CONSTRAINT_TOO_MANY, variable.reference());
            } else { // variable.relation().isEmpty()
                throw GraknException.of(RELATION_CONSTRAINT_MISSING, variable.reference());
            }
        }
    }

    private void insertHas(Thing thing, Set<HasConstraint> hasConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "inserthas")) {
            hasConstraints.forEach(has -> thing.setHas(insert(has.attribute()).asAttribute()));
        }
    }
}
