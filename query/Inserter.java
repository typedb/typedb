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

package com.vaticle.typedb.core.query;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
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
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.LabelConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
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

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ATTRIBUTE_VALUE_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ABSTRACT_WRITE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_INSERT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INSERT_RELATION_CONSTRAINT_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
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
import static com.vaticle.typedb.core.query.common.Util.getRoleType;

public class Inserter {

    private static final String TRACE_PREFIX = "inserter.";

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
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            VariableRegistry registry = VariableRegistry.createFromThings(query.variables());
            registry.variables().forEach(Inserter::validate);

            Matcher matcher = null;
            if (query.match().isPresent()) {
                TypeQLMatch.Unfiltered match = query.match().get();
                List<UnboundVariable> filter = new ArrayList<>(match.namedVariablesUnbound());
                filter.retainAll(query.namedVariablesUnbound());
                assert !filter.isEmpty();
                matcher = Matcher.create(reasoner, match.get(filter));
            }

            return new Inserter(matcher, conceptMgr, registry.things(), context);
        }
    }

    public static void validate(Variable var) {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
            if (var.isType()) validate(var.asType());
            else validate(var.asThing());
        }
    }

    private static void validate(TypeVariable var) {
        if (!var.reference().isLabel()) throw TypeDBException.of(ILLEGAL_TYPE_VARIABLE_IN_INSERT, var.reference());
    }

    private static void validate(ThingVariable var) {
        Identifier id = var.id();
        if (var.iid().isPresent()) {
            throw TypeDBException.of(THING_IID_NOT_INSERTABLE, id, var.iid().get());
        } else if (!var.is().isEmpty()) {
            throw TypeDBException.of(ILLEGAL_IS_CONSTRAINT, var, var.is().iterator().next());
        }
    }

    public FunctionalIterator<ConceptMap> execute() {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            if (matcher != null) return context.options().parallel() ? executeParallel() : executeSerial();
            else return single(new Operation(conceptMgr, new ConceptMap(), variables).execute());
        }
    }

    private FunctionalIterator<ConceptMap> executeParallel() {
        List<List<ConceptMap>> lists = matcher.execute(context).toLists(PARALLELISATION_SPLIT_MIN, PARALLELISATION_FACTOR);
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
        List<ConceptMap> matches = matcher.execute(context).toList();
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
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
                variables.forEach(this::insert);
                matched.forEach((id, concept) -> inserted.putIfAbsent(id, concept.asThing()));
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
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert")) {
                assert var.id().isRetrievable(); // thing variables are always retrieved
                Thing thing;
                Retrievable id = var.id();

                if (id.isName() && (thing = inserted.get(id)) != null) return thing;
                else if (matchedContains(var) && var.constraints().isEmpty()) return matchedGet(var);

                if (matchedContains(var)) {
                    thing = matchedGet(var);
                    if (var.isa().isPresent() && !thing.getType().equals(getThingType(var.isa().get().type().label().get()))) {
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
        }

        public ThingType getThingType(LabelConstraint labelConstraint) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_thing_type")) {
                ThingType thingType = conceptMgr.getThingType(labelConstraint.label());
                if (thingType == null) throw TypeDBException.of(TYPE_NOT_FOUND, labelConstraint.label());
                else return thingType.asThingType();
            }
        }

        private Thing insertIsa(IsaConstraint isaConstraint, ThingVariable var) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_isa")) {
                assert isaConstraint.type().label().isPresent();
                ThingType thingType = getThingType(isaConstraint.type().label().get());

                if (thingType.isEntityType()) {
                    return thingType.asEntityType().create();
                } else if (thingType.isRelationType()) {
                    if (var.relation().isPresent()) return thingType.asRelationType().create();
                    else throw TypeDBException.of(RELATION_CONSTRAINT_MISSING, var.reference());
                } else if (thingType.isAttributeType()) {
                    return insertAttribute(thingType.asAttributeType(), var);
                } else if (thingType.isThingType() && thingType.isRoot()) {
                    throw TypeDBException.of(ILLEGAL_ABSTRACT_WRITE, Thing.class.getSimpleName(), thingType.getLabel());
                } else {
                    assert false;
                    return null;
                }
            }
        }

        private Attribute insertAttribute(AttributeType attributeType, ThingVariable var) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_attribute")) {
                ValueConstraint<?> valueConstraint;

                if (var.value().size() > 1) {
                    throw TypeDBException.of(ATTRIBUTE_VALUE_TOO_MANY, var.reference(), attributeType.getLabel());
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
                    throw TypeDBException.of(ATTRIBUTE_VALUE_MISSING, var.reference(), attributeType.getLabel());
                }
            }
        }

        private void insertRolePlayers(Relation relation, ThingVariable var) {
            assert var.relation().isPresent();
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "extend_relation")) {
                if (var.relation().isPresent()) {
                    var.relation().get().players().forEach(rolePlayer -> {
                        Thing player = insert(rolePlayer.player());
                        RoleType roleType = getRoleType(relation, player, rolePlayer);
                        relation.addPlayer(roleType, player);
                    });
                } else { // var.relation().size() > 1
                    throw TypeDBException.of(INSERT_RELATION_CONSTRAINT_TOO_MANY, var.reference());
                }
            }
        }

        private void insertHas(Thing thing, Set<HasConstraint> hasConstraints) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "insert_has")) {
                hasConstraints.forEach(has -> thing.setHas(insert(has.attribute()).asAttribute()));
            }
        }
    }
}
