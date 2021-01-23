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
import grakn.core.concept.type.impl.ThingTypeImpl;
import grakn.core.pattern.constraint.thing.HasConstraint;
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
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_INSERT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_REINSERTION;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;
import static java.util.stream.Collectors.toSet;

public class Inserter {

    private static final String TRACE_PREFIX = "inserter.";

    private final ConceptManager conceptMgr;
    private final Context.Query context;
    private final ConceptMap matched;
    private final Map<Reference.Name, Thing> inserted;
    private final Set<ThingVariable> variables;

    private Inserter(ConceptManager conceptMgr, Set<ThingVariable> vars, ConceptMap matched, Context.Query context) {
        this.conceptMgr = conceptMgr;
        this.variables = vars;
        this.context = context;
        this.matched = matched;
        this.inserted = new HashMap<>();
    }

    public static Inserter create(ConceptManager conceptMgr, List<graql.lang.pattern.variable.ThingVariable<?>> vars,
                                  Context.Query context) {
        return create(conceptMgr, vars, new ConceptMap(), context);
    }

    public static Inserter create(ConceptManager conceptMgr, List<graql.lang.pattern.variable.ThingVariable<?>> vars,
                                  ConceptMap matched, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            VariableRegistry registry = VariableRegistry.createFromThings(vars);
            iterate(registry.types()).filter(t -> !t.reference().isLabel()).forEachRemaining(t -> {
                throw GraknException.of(ILLEGAL_TYPE_VARIABLE_IN_INSERT, t.reference());
            });
            return new Inserter(conceptMgr, registry.things(), matched, context);
        }
    }

    public ConceptMap execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            variables.forEach(this::insert);
            matched.forEach((ref, concept) -> inserted.putIfAbsent(ref, concept.asThing()));
            return new ConceptMap(inserted);
        }
    }

    private boolean matchedContains(ThingVariable var) {
        return var.reference().isName() && matched.contains(var.reference().asName());
    }

    public Thing matchedGet(ThingVariable var) {
        return matched.get(var.reference().asName()).asThing();
    }

    private Thing insert(ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
            Thing thing; Reference ref = var.reference();

            if (ref.isName() && (thing = inserted.get(ref.asName())) != null) return thing;
            else if (matchedContains(var) && var.constraints().isEmpty()) return matchedGet(var);
            else validate(var);

            if (matchedContains(var)) {
                thing = matchedGet(var);
                if (var.isa().isPresent() && !thing.getType().equals(getThingType(var.isa().get().type()))) {
                    throw GraknException.of(THING_ISA_REINSERTION, ref, var.isa().get().type());
                }
            } else if (var.isa().isPresent()) thing = insertIsa(var.isa().get(), var);
            else throw GraknException.of(THING_ISA_MISSING, ref);
            assert thing != null;
            
            if (ref.isName()) inserted.put(ref.asName(), thing);
            if (!var.relation().isEmpty()) extendRelation(thing.asRelation(), var);
            if (!var.has().isEmpty()) insertHas(thing, var.has());
            return thing;
        }
    }

    private void validate(ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
            Reference ref = var.reference();
            if (var.iid().isPresent()) {
                throw GraknException.of(THING_IID_NOT_INSERTABLE, ref, var.iid().get());
            } else if (!var.is().isEmpty()) {
                throw GraknException.of(ILLEGAL_IS_CONSTRAINT, var, var.is().iterator().next());
            }
        }
    }

    private ThingType getThingType(TypeVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_thing_type")) {
            assert var.reference().isLabel() && var.label().isPresent();
            final ThingType thingType = conceptMgr.getThingType(var.label().get().label());
            if (thingType == null) throw GraknException.of(TYPE_NOT_FOUND, var.label().get().label());
            else return thingType.asThingType();
        }
    }

    private RoleType getRoleType(TypeVariable var) {
        return getRoleType(var, null);
    }

    private RoleType getRoleType(TypeVariable var, RelationType relation) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_role_type")) {
            assert var.reference().isLabel() && var.label().isPresent();
            final RelationType relationType = relation != null ?
                    relation : conceptMgr.getRelationType(var.label().get().scope().get());
            final RoleType roleType;
            if ((relationType != null && (roleType = relationType.getRelates(var.label().get().label())) != null)) {
                return roleType;
            } else {
                throw GraknException.of(TYPE_NOT_FOUND, var.label().get().scopedLabel());
            }
        }
    }

    private Thing insertIsa(IsaConstraint isaConstraint, ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_isa")) {
            final ThingType thingType = getThingType(isaConstraint.type());

            if (thingType instanceof EntityType) {
                return insertEntity(thingType.asEntityType());
            } else if (thingType instanceof AttributeType) {
                return insertAttribute(thingType.asAttributeType(), var);
            } else if (thingType instanceof RelationType) {
                return insertRelation(thingType.asRelationType(), var);
            } else if (thingType instanceof ThingTypeImpl.Root) {
                throw GraknException.of(ILLEGAL_ABSTRACT_WRITE, Thing.class.getSimpleName(), thingType.getLabel());
            } else {
                assert false;
                return null;
            }
        }
    }

    private Entity insertEntity(EntityType entityType) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_entity")) {
            return entityType.create();
        }
    }

    private Attribute insertAttribute(AttributeType attributeType, ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_attribute")) {
            ValueConstraint<?> valueConstraint;

            if (var.value().size() > 1) {
                throw GraknException.of(ATTRIBUTE_VALUE_TOO_MANY, var.reference(), attributeType.getLabel());
            } else if (!var.value().isEmpty() &&
                    (valueConstraint = var.value().iterator().next()).isValueIdentity()) {
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
                throw GraknException.of(ATTRIBUTE_VALUE_MISSING, var.reference(), attributeType.getLabel());
            }
        }
    }

    private Relation insertRelation(RelationType relationType, ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_relation")) {
            return relationType.create();
        }
    }

    private Relation extendRelation(Relation relation, ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "extend_relation")) {
            if (var.relation().size() == 1) {
                var.relation().iterator().next().players().forEach(rolePlayer -> {
                    final RoleType roleType;
                    final Thing player = insert(rolePlayer.player());
                    final Set<RoleType> inferred;
                    if (rolePlayer.roleType().isPresent()) {
                        roleType = getRoleType(rolePlayer.roleType().get(), relation.getType());
                    } else if ((inferred = player.getType().getPlays()
                            .filter(rt -> rt.getRelationType().equals(relation.getType()))
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
            } else if (var.relation().size() > 1) {
                throw GraknException.of(RELATION_CONSTRAINT_TOO_MANY, var.reference());
            } else { // var.relation().isEmpty()
                throw GraknException.of(RELATION_CONSTRAINT_MISSING, var.reference());
            }
        }
    }

    private void insertHas(Thing thing, Set<HasConstraint> hasConstraints) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_has")) {
            hasConstraints.forEach(has -> thing.setHas(insert(has.attribute()).asAttribute()));
        }
    }
}
