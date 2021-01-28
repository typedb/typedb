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
 */

package grakn.core.query;

import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RoleType;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.reasoner.Reasoner;
import graql.lang.pattern.variable.Reference;
import graql.lang.query.GraqlDelete;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.DELETE_RELATION_CONSTRAINT_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_RELATION_IN_DELETE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_DELETE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_HAS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING_DIRECT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static grakn.core.query.common.Util.getRoleType;

public class Deleter {

    private static final String TRACE_PREFIX = "deleter.";

    private final Matcher matcher;
    private final ConceptManager conceptMgr;
    private final Set<ThingVariable> variables;
    private final Context.Query context;

    public Deleter(Matcher matcher, ConceptManager conceptMgr, Set<ThingVariable> variables, Context.Query context) {
        this.matcher = matcher;
        this.conceptMgr = conceptMgr;
        this.variables = variables;
        this.context = context;
        this.context.producer(EXHAUSTIVE);
    }

    public static Deleter create(Reasoner reasoner, ConceptManager conceptMgr, GraqlDelete query, Context.Query context) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            VariableRegistry registry = VariableRegistry.createFromThings(query.variables(), false);
            iterate(registry.types()).filter(t -> !t.reference().isLabel()).forEachRemaining(t -> {
                throw GraknException.of(ILLEGAL_TYPE_VARIABLE_IN_DELETE, t.reference());
            });

            assert query.match().namedVariablesUnbound().containsAll(query.namedVariablesUnbound());
            Matcher matcher = Matcher.create(reasoner, query.match().get(query.namedVariablesUnbound()));
            return new Deleter(matcher, conceptMgr, registry.things(), context);
        }
    }

    public void execute() {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            List<ConceptMap> matches = matcher.execute(context).onError(conceptMgr::exception).toList();
            matches.forEach(matched -> new Operation(matched).execute());
        }
    }

    private class Operation {

        private static final String TRACE_PREFIX = "operation.";

        private final ConceptMap matched;
        private final Map<ThingVariable, Thing> detached;

        private Operation(ConceptMap matched) {
            this.matched = matched;
            this.detached = new HashMap<>();
        }

        private void execute() {
            try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
                variables.forEach(this::delete);
                variables.forEach(this::deleteIsa);
            }
        }

        private void delete(ThingVariable var) {
            try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
                validate(var);
                Thing thing = matched.get(var.reference().asName()).asThing();
                if (!var.has().isEmpty()) deleteHas(var, thing);
                if (!var.relation().isEmpty()) deleteRelation(var, thing.asRelation());
                detached.put(var, thing);
            }
        }

        private void validate(ThingVariable var) {
            try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
                if (!var.reference().isName()) {
                    ErrorMessage.ThingWrite msg = !var.relation().isEmpty()
                            ? ILLEGAL_ANONYMOUS_RELATION_IN_DELETE
                            : ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
                    throw GraknException.of(msg, var);
                } else if (var.iid().isPresent()) {
                    throw GraknException.of(THING_IID_NOT_INSERTABLE, var.reference(), var.iid().get());
                } else if (!var.is().isEmpty()) {
                    throw GraknException.of(ILLEGAL_IS_CONSTRAINT, var, var.is().iterator().next());
                }
            }
        }

        private void deleteHas(ThingVariable var, Thing thing) {
            try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_has")) {
                for (HasConstraint hasConstraint : var.has()) {
                    Reference.Name attRef = hasConstraint.attribute().reference().asName();
                    Attribute att = matched.get(attRef).asAttribute();
                    if (thing.getHas(att.getType()).anyMatch(a -> a.equals(att))) thing.unsetHas(att);
                    else throw GraknException.of(INVALID_DELETE_HAS, var.reference(), attRef);
                }
            }
        }

        private void deleteRelation(ThingVariable var, Relation relation) {
            try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_relation")) {
                if (var.relation().size() == 1) {
                    var.relation().iterator().next().players().forEach(rolePlayer -> {
                        Thing player = matched.get(rolePlayer.player().reference().asName()).asThing();
                        RoleType roleType = getRoleType(relation, player, rolePlayer);
                        relation.removePlayer(roleType, player);
                    });
                } else {
                    throw GraknException.of(DELETE_RELATION_CONSTRAINT_TOO_MANY, var.reference());
                }
            }
        }

        private void deleteIsa(ThingVariable var) {
            try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_isa")) {
                Thing thing = detached.get(var);
                if (var.isa().isPresent() && !thing.isDeleted()) {
                    Label typeLabel = var.isa().get().type().label().get().properLabel();
                    if (var.isa().get().isExplicit()) {
                        if (thing.getType().getLabel().equals(typeLabel)) thing.delete();
                        else throw GraknException.of(INVALID_DELETE_THING_DIRECT, var.reference(), typeLabel);
                    } else {
                        if (thing.getType().getSupertypes().anyMatch(t -> t.getLabel().equals(typeLabel)))
                            thing.delete();
                        else throw GraknException.of(INVALID_DELETE_THING, var.reference(), typeLabel);
                    }
                }
            }
        }
    }
}
