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
 */

package com.vaticle.typedb.core.query;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import com.vaticle.typeql.lang.query.TypeQLDelete;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.DELETE_RELATION_CONSTRAINT_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_RELATION_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_HAS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING_DIRECT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.query.common.Util.getRoleType;

public class Deleter {

    private static final String TRACE_PREFIX = "deleter.";

    private final Matcher matcher;
    private final Set<ThingVariable> variables;
    private final Context.Query context;

    public Deleter(Matcher matcher, Set<ThingVariable> variables, Context.Query context) {
        this.matcher = matcher;
        this.variables = variables;
        this.context = context;
        this.context.producer(Either.first(EXHAUSTIVE));
    }

    public static Deleter create(Reasoner reasoner, TypeQLDelete query, Context.Query context) {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            VariableRegistry registry = VariableRegistry.createFromThings(query.variables(), false);
            registry.variables().forEach(Deleter::validate);

            assert query.match().namedVariablesUnbound().containsAll(query.namedVariablesUnbound());
            Matcher matcher = Matcher.create(reasoner, query.match().get(query.namedVariablesUnbound()));
            return new Deleter(matcher, registry.things(), context);
        }
    }

    public static void validate(Variable var) {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
            if (var.isType()) validate(var.asType());
            else validate(var.asThing());
        }
    }

    private static void validate(TypeVariable var) {
        if (!var.reference().isLabel()) throw TypeDBException.of(ILLEGAL_TYPE_VARIABLE_IN_DELETE, var.reference());
    }

    private static void validate(ThingVariable var) {
        if (!var.reference().isName()) {
            ErrorMessage.ThingWrite msg = var.relation().isPresent()
                    ? ILLEGAL_ANONYMOUS_RELATION_IN_DELETE
                    : ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
            throw TypeDBException.of(msg, var);
        } else if (var.iid().isPresent()) {
            throw TypeDBException.of(THING_IID_NOT_INSERTABLE, var.reference(), var.iid().get());
        } else if (!var.is().isEmpty()) {
            throw TypeDBException.of(ILLEGAL_IS_CONSTRAINT, var, var.is().iterator().next());
        }
    }

    public void execute() {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            List<ConceptMap> matches = matcher.execute(context).toList();
            matches.forEach(matched -> new Operation(matched, variables).execute());
        }
    }

    static class Operation {

        private static final String TRACE_PREFIX = "operation.";

        private final ConceptMap matched;
        private final Set<ThingVariable> variables;
        private final Map<ThingVariable, Thing> detached;

        Operation(ConceptMap matched, Set<ThingVariable> variables) {
            this.matched = matched;
            this.variables = variables;
            this.detached = new HashMap<>();
        }

        void execute() {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
                variables.forEach(this::delete);
                variables.forEach(this::deleteIsa);
            }
        }

        private void delete(ThingVariable var) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
                validate(var);
                Thing thing = matched.get(var.reference().asName()).asThing();
                if (!var.has().isEmpty()) deleteHas(var, thing);
                if (var.relation().isPresent()) deleteRelation(var, thing.asRelation());
                detached.put(var, thing);
            }
        }


        private void deleteHas(ThingVariable var, Thing thing) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_has")) {
                for (HasConstraint hasConstraint : var.has()) {
                    Reference.Name attRef = hasConstraint.attribute().reference().asName();
                    Attribute att = matched.get(attRef).asAttribute();
                    if (thing.getHas(att.getType()).anyMatch(a -> a.equals(att))) thing.unsetHas(att);
                    else throw TypeDBException.of(INVALID_DELETE_HAS, var.reference(), attRef);
                }
            }
        }

        private void deleteRelation(ThingVariable var, Relation relation) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_relation")) {
                if (var.relation().isPresent()) {
                    var.relation().get().players().forEach(rolePlayer -> {
                        Thing player = matched.get(rolePlayer.player().reference().asName()).asThing();
                        RoleType roleType = getRoleType(relation, player, rolePlayer);
                        relation.removePlayer(roleType, player);
                    });
                } else {
                    throw TypeDBException.of(DELETE_RELATION_CONSTRAINT_TOO_MANY, var.reference());
                }
            }
        }

        private void deleteIsa(ThingVariable var) {
            try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_isa")) {
                Thing thing = detached.get(var);
                ThingType type = thing.getType();
                if (var.isa().isPresent() && !thing.isDeleted()) {
                    Label typeLabel = var.isa().get().type().label().get().properLabel();
                    if (var.isa().get().isExplicit()) {
                        if (type.getLabel().equals(typeLabel)) thing.delete();
                        else throw TypeDBException.of(INVALID_DELETE_THING_DIRECT, var.reference(), typeLabel);
                    } else {
                        if (type.getSupertypes().anyMatch(t -> t.getLabel().equals(typeLabel))) thing.delete();
                        else throw TypeDBException.of(INVALID_DELETE_THING, var.reference(), typeLabel);
                    }
                }
            }
        }
    }
}
