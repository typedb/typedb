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
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_RELATION_IN_DELETE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_TYPE_VARIABLE_IN_DELETE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_HAS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_CONSTRAINT_TOO_MANY;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_AMBIGUOUS;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.common.iterator.Iterators.iterate;
import static java.util.stream.Collectors.toSet;

public class Deleter {

    private static final String TRACE_PREFIX = "deleter.";

    private final ConceptManager conceptMgr;
    private final Context.Query context;
    private final ConceptMap matched;
    private final Set<ThingVariable> variables;
    private final Map<ThingVariable, Thing> detached;

    private Deleter(ConceptManager conceptMgr, Set<ThingVariable> vars, ConceptMap matched, Context.Query context) {
        this.conceptMgr = conceptMgr;
        this.context = context;
        this.matched = matched;
        this.variables = vars;
        this.detached = new HashMap<>();
    }

    public static Deleter create(ConceptManager conceptMgr, List<graql.lang.pattern.variable.ThingVariable<?>> vars,
                                 ConceptMap matched, Context.Query context) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "create")) {
            VariableRegistry registry = VariableRegistry.createFromThings(vars);
            iterate(registry.types()).filter(t -> !t.reference().isLabel()).forEachRemaining(t -> {
                throw GraknException.of(ILLEGAL_TYPE_VARIABLE_IN_DELETE, t.reference());
            });
            return new Deleter(conceptMgr, VariableRegistry.createFromThings(vars).things(), matched, context);
        }
    }

    public void execute() {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "execute")) {
            variables.forEach(this::delete);
            variables.forEach(this::delete_isa);
        }
    }

    private void delete(ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete")) {
            validate(var);
            Thing thing = matched.get(var.reference().asName()).asThing();
            if (!var.has().isEmpty()) deleteHas(var, thing);
            if (!var.relation().isEmpty()) deleteRelation(var, thing.asRelation());
            detached.put(var, thing);
        }
    }

    private void validate(ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "validate")) {
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
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_has")) {
            for (HasConstraint hasConstraint : var.has()) {
                Reference.Name attRef = hasConstraint.attribute().reference().asName();
                Attribute att = matched.get(attRef).asAttribute();
                if (thing.getHas(att.getType()).anyMatch(a -> a.equals(att))) thing.unsetHas(att);
                else throw GraknException.of(INVALID_DELETE_HAS, var.reference(), attRef);
            }
        }
    }

    private void deleteRelation(ThingVariable var, Relation relation) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_relation")) {
            if (var.relation().size() == 1) {
                var.relation().iterator().next().players().forEach(rolePlayer -> {
                    RoleType roleType;
                    Thing player = matched.get(rolePlayer.player().reference().asName()).asThing();
                    Set<RoleType> inferred;
                    if (rolePlayer.roleType().isPresent()) {
                        roleType = getRoleType(rolePlayer.roleType().get());
                    } else if ((inferred = player.getType().getPlays()
                            .filter(rt -> rt.getRelationType().equals(relation.getType()))
                            .collect(toSet())).size() == 1) {
                        roleType = inferred.iterator().next();
                    } else if (inferred.size() > 1) {
                        throw GraknException.of(ROLE_TYPE_AMBIGUOUS, rolePlayer.player().reference());
                    } else {
                        throw GraknException.of(ROLE_TYPE_MISSING, rolePlayer.player().reference());
                    }

                    relation.removePlayer(roleType, player);
                });
            } else {
                throw GraknException.of(RELATION_CONSTRAINT_TOO_MANY, var.reference());
            }
        }
    }

    private void delete_isa(ThingVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "delete_isa")) {
            Thing thing = detached.get(var);
            if (var.isa().isPresent() && !thing.isDeleted()) {
                Label typeLabel = var.isa().get().type().label().get().properLabel();
                if (thing.getType().getSupertypes().anyMatch(t -> t.getLabel().equals(typeLabel))) thing.delete();
                else throw GraknException.of(INVALID_DELETE_THING, var.reference(), typeLabel);
            }
        }
    }

    private RoleType getRoleType(TypeVariable var) {
        try (ThreadTrace ignored = traceOnThread(TRACE_PREFIX + "get_role_type")) {
            assert var.reference().isLabel() && var.label().isPresent();
            final RelationType relationType;
            final RoleType roleType;
            if ((relationType = conceptMgr.getRelationType(var.label().get().scope().get())) != null &&
                    (roleType = relationType.getRelates(var.label().get().label())) != null) {
                return roleType;
            } else {
                throw GraknException.of(TYPE_NOT_FOUND, var.label().get().scopedLabel());
            }
        }
    }
}
