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

package com.vaticle.typedb.core.query;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.pattern.constraint.common.Predicate;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.ValueVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_UNBOUND_TYPE_VAR_IN_INSERT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_VALUE_CONSTRAINT_IN_INSERT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INSERT_RELATION_CONSTRAINT_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_INSERT_ISA_NOT_THING_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_REINSERTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.single;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.query.QueryManager.PARALLELISATION_SPLIT_MIN;
import static com.vaticle.typedb.core.query.common.Util.tryInferRoleType;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.Equality.EQ;

public class Inserter {

    private final Matcher matcher;
    private final ConceptManager conceptMgr;
    private final Set<ThingVariable> variables;
    private final Context.Query context;

    public Inserter(@Nullable Matcher matcher, ConceptManager conceptMgr,
                    Set<ThingVariable> variables, Context.Query context) {
        this.matcher = matcher;
        this.conceptMgr = conceptMgr;
        this.variables = variables;
        this.context = context;
        this.context.producer(Either.first(EXHAUSTIVE));
    }

    public static Inserter create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLInsert query, Context.Query context) {
        Matcher matcher = null;
        if (query.match().isPresent()) {
            TypeQLMatch.Unfiltered match = query.match().get();
            List<UnboundVariable> filter = new ArrayList<>(match.namedVariablesUnbound());
            filter.retainAll(query.namedVariablesUnbound());
            assert !filter.isEmpty();
            matcher = Matcher.create(reasoner, match.get(filter));
        }
        VariableRegistry registry = VariableRegistry.createFromThings(query.variables());
        for (Variable variable : registry.variables()) validate(variable, matcher);
        return new Inserter(matcher, conceptMgr, registry.things(), context);
    }

    public static void validate(Variable var, @Nullable Matcher matcher) {
        if (var.isType()) validate(var.asType(), matcher);
        else if (var.isThing()) validate(var.asThing());
        else if (var.isValue()) validate(var.asValue());
    }

    private static void validate(TypeVariable var, @Nullable Matcher matcher) {
        if (var.id().isName() &&
                (matcher == null || !matcher.disjunction().sharedVariables().contains(var.id().asName()))) {
            throw TypeDBException.of(ILLEGAL_UNBOUND_TYPE_VAR_IN_INSERT, var.id());
        }
    }

    private static void validate(ThingVariable var) {
        Identifier id = var.id();
        if (var.iid().isPresent()) {
            throw TypeDBException.of(THING_IID_NOT_INSERTABLE, id, var.iid().get());
        } else if (!var.is().isEmpty()) {
            throw TypeDBException.of(ILLEGAL_IS_CONSTRAINT, var, var.is().iterator().next());
        }
    }

    private static void validate(ValueVariable var) {
        var.constraints().forEach(constraint -> {
            if (!(constraint.isThing() && constraint.asThing().isPredicate() && constraint.asThing().asPredicate().predicate().predicate().equals(EQ))) {
                throw TypeDBException.of(ILLEGAL_VALUE_CONSTRAINT_IN_INSERT, var.reference());
            }
        });
        var.constraining().forEach(constraint -> {
            if (!(constraint.isThing() && constraint.asThing().isPredicate() && constraint.asThing().asPredicate().predicate().predicate().equals(EQ))) {
                throw TypeDBException.of(ILLEGAL_VALUE_CONSTRAINT_IN_INSERT, var.reference());
            }
        });
    }

    public FunctionalIterator<ConceptMap> execute() {
        if (matcher != null) return context.options().parallel() ? executeParallel() : executeSerial();
        else return single(new Operation(conceptMgr, new ConceptMap(), variables).execute());
    }

    private FunctionalIterator<ConceptMap> executeParallel() {
        List<? extends List<? extends ConceptMap>> lists = matcher.execute(context).toLists(PARALLELISATION_SPLIT_MIN, PARALLELISATION_FACTOR);
        assert !lists.isEmpty();
        List<ConceptMap> inserts;
        if (lists.size() == 1) inserts = iterate(lists.get(0)).map(
                matched -> new Operation(conceptMgr, matched, variables).execute()
        ).toList();
        else inserts = produce(async(iterate(lists).map(list -> iterate(list).map(
                matched -> new Operation(conceptMgr, matched, variables).execute()
        )), PARALLELISATION_FACTOR), Either.first(EXHAUSTIVE), async1()).toList();
        return iterate(inserts);
    }

    private FunctionalIterator<ConceptMap> executeSerial() {
        List<? extends ConceptMap> matches = matcher.execute(context).toList();
        return iterate(iterate(matches).map(matched -> new Operation(conceptMgr, matched, variables).execute()).toList());
    }

    public static class Operation {

        private static final String TRACE_PREFIX = "operation.";

        private final ConceptManager conceptMgr;
        private final ConceptMap matched;
        private final Set<ThingVariable> variables;
        private final Map<Retrievable, Thing> inserted;

        Operation(ConceptManager conceptMgr, ConceptMap matched, Set<ThingVariable> variables) {
            this.conceptMgr = conceptMgr;
            this.matched = matched;
            this.variables = variables;
            this.inserted = new HashMap<>();
        }

        public ConceptMap execute() {
            variables.forEach(this::insert);
            matched.forEach((id, concept) -> {
                if (concept.isThing()) inserted.putIfAbsent(id, concept.asThing());
            });
            return new ConceptMap(inserted);
        }

        private boolean matchedContains(Variable var) {
            return var.reference().isNameConcept() && matched.contains(var.reference().asName().asConcept());
        }

        public Thing matchedGet(ThingVariable var) {
            return matched.get(var.reference().asName()).asThing();
        }

        public Type matchedGet(TypeVariable var) {
            return matched.get(var.reference().asName()).asType();
        }

        private Thing insert(ThingVariable var) {
            assert var.id().isRetrievable(); // thing variables are always retrieved
            Thing thing;
            Retrievable id = var.id();

            if (id.isName() && (thing = inserted.get(id)) != null) return thing;
            else if (matchedContains(var) && var.constraints().isEmpty()) return matchedGet(var);

            if (matchedContains(var)) {
                thing = matchedGet(var);
                if (var.isa().isPresent() && !thing.getType().equals(getType(var.isa().get().type()))) {
                    throw TypeDBException.of(THING_ISA_REINSERTION, id, var.isa().get().type());
                }
            } else if (var.isa().isPresent()) thing = insertIsa(var.isa().get(), var);
            else throw TypeDBException.of(THING_ISA_MISSING, id);
            assert thing != null;

            inserted.put(id, thing);
            if (var.relation().isPresent()) insertRolePlayers(thing.asRelation(), var);
            if (!var.has().isEmpty()) insertHas(thing, var.has());
            return thing;
        }

        public Type getType(TypeVariable variable) {
            if (matchedContains(variable)) {
                return matchedGet(variable.asType());
            } else {
                return getThingType(variable.label().get());
            }
        }

        public ThingType getThingType(LabelConstraint labelConstraint) {
            ThingType thingType = conceptMgr.getThingType(labelConstraint.label());
            if (thingType == null) throw TypeDBException.of(TYPE_NOT_FOUND, labelConstraint.label());
            else return thingType.asThingType();
        }

        private Thing insertIsa(IsaConstraint isaConstraint, ThingVariable var) {
            Type type = getType(isaConstraint.type());
            if (!type.isThingType()) {
                throw TypeDBException.of(THING_INSERT_ISA_NOT_THING_TYPE, type.getLabel());
            }

            if (type.isEntityType()) {
                return type.asEntityType().create();
            } else if (type.isRelationType()) {
                if (var.relation().isPresent()) return type.asRelationType().create();
                else throw TypeDBException.of(RELATION_CONSTRAINT_MISSING, var.reference());
            } else if (type.isAttributeType()) {
                return insertAttribute(type.asAttributeType(), var);
            } else if (type.isThingType() && type.isRoot()) {
                throw TypeDBException.of(ILLEGAL_ABSTRACT_WRITE, Thing.class.getSimpleName(), type.getLabel());
            } else {
                assert false;
                return null;
            }
        }

        private Attribute insertAttribute(AttributeType attributeType, ThingVariable var) {
            if (var.predicates().size() > 1) {
                throw TypeDBException.of(ATTRIBUTE_VALUE_TOO_MANY, var.reference(), attributeType.getLabel());
            } else if (var.predicates().isEmpty()) {
                throw TypeDBException.of(ATTRIBUTE_VALUE_MISSING, var.reference(), attributeType.getLabel());
            } else {
                Predicate<?> predicate = var.predicates().iterator().next().predicate();
                if (predicate.predicate().equals(EQ) && predicate.isConstant()) {
                    switch (attributeType.getValueType()) {
                        case LONG:
                            return attributeType.asLong().put(predicate.asConstant().asLong().value());
                        case DOUBLE:
                            return attributeType.asDouble().put(predicate.asConstant().asDouble().value());
                        case BOOLEAN:
                            return attributeType.asBoolean().put(predicate.asConstant().asBoolean().value());
                        case STRING:
                            return attributeType.asString().put(predicate.asConstant().asString().value());
                        case DATETIME:
                            return attributeType.asDateTime().put(predicate.asConstant().asDateTime().value());
                        default:
                            assert false;
                            return null;
                    }
                } else if (predicate.predicate().equals(EQ) && predicate.isValueVar()) {
                    Value<?> value = matched.get(predicate.asValueVar().value().id()).asValue();
                    switch (attributeType.getValueType()) {
                        case LONG:
                            return attributeType.asLong().put(value.asLong().value());
                        case DOUBLE:
                            return attributeType.asDouble().put(value.asDouble().value());
                        case BOOLEAN:
                            return attributeType.asBoolean().put(value.asBoolean().value());
                        case STRING:
                            return attributeType.asString().put(value.asString().value());
                        case DATETIME:
                            return attributeType.asDateTime().put(value.asDateTime().value());
                        default:
                            assert false;
                            return null;
                    }
                } else throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private void insertRolePlayers(Relation relation, ThingVariable var) {
            assert var.relation().isPresent();
            if (var.relation().isPresent()) {
                var.relation().get().players().forEach(rolePlayer -> {
                    Thing player = insert(rolePlayer.player());
                    RoleType roleType;
                    if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().id().isName()) {
                        Type type = getType(rolePlayer.roleType().get());
                        if (!type.isRoleType()) throw TypeDBException.of(ROLE_TYPE_MISMATCH, type.getLabel());
                        else roleType = type.asRoleType();
                    } else {
                        roleType = tryInferRoleType(relation, player, rolePlayer);
                    }
                    relation.addPlayer(roleType, player);
                });
            } else { // var.relation().size() > 1
                throw TypeDBException.of(INSERT_RELATION_CONSTRAINT_TOO_MANY, var.reference());
            }
        }

        private void insertHas(Thing thing, Set<HasConstraint> hasConstraints) {
            hasConstraints.forEach(has -> thing.setHas(insert(has.attribute()).asAttribute()));
        }
    }
}
