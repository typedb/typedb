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
 */

package com.vaticle.typedb.core.query;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typeql.lang.common.Reference;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.pattern.constraint.Constraint;
import com.vaticle.typeql.lang.pattern.statement.Statement;
import com.vaticle.typeql.lang.query.TypeQLDelete;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.DELETE_RELATION_CONSTRAINT_TOO_MANY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.HAS_TYPE_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_IS_CONSTRAINT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ILLEGAL_VALUE_VARIABLE_IN_DELETE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.INVALID_DELETE_THING_DIRECT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.PLAYING_TYPE_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.RELATING_TYPE_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.ROLE_TYPE_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_IID_NOT_INSERTABLE;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.query.common.Util.tryInferRoleType;

public class Deleter {

    private final Getter getter;
    private final Set<ThingVariable> variables;
    private final Context.Query context;

    public Deleter(Getter getter, Set<ThingVariable> variables, Context.Query context) {
        this.getter = getter;
        this.variables = variables;
        this.context = context;
        this.context.producer(Either.first(EXHAUSTIVE));
    }

    public static Deleter create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLDelete query, Context.Query context) {
        validateTypeQLVariables(query);
        VariableRegistry registry = VariableRegistry.createFromThings(query.statements(), false);
        registry.variables().forEach(Deleter::validate);
        assert query.match().get().namedVariables().containsAll(query.namedVariables());

        Set<TypeQLVariable> filter = new HashSet<>(query.match().get().namedVariables());
        filter.retainAll(query.namedVariables());
        assert !filter.isEmpty();
        if (query.modifiers().sort().isPresent()) {
            filter.addAll(query.modifiers().sort().get().variables());
        }

        Getter getter = Getter.create(reasoner, conceptMgr, query.match().get().get(new ArrayList<>(filter)).modifiers(query.modifiers()), context);
        return new Deleter(getter, registry.things(), context);
    }

    private static void validateTypeQLVariables(TypeQLDelete query) {
        for (Statement statement : query.statements()) {
            if (statement.headVariable().isAnonymised()) {
                throw TypeDBException.of(ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE, statement);
            }
            for (Constraint constraint : statement.constraints()) {
                Optional<? extends TypeQLVariable> var = constraint.variables().stream().filter(v ->  v.isValueVar() || v.isAnonymised()).findFirst();
                if (var.isPresent()) {
                    if (var.get().isValueVar()) {
                        throw TypeDBException.of(ILLEGAL_VALUE_VARIABLE_IN_DELETE, var.get());
                    } else if (var.get().isAnonymised()) {
                        throw TypeDBException.of(ILLEGAL_ANONYMOUS_VARIABLE_IN_DELETE, constraint);
                    }
                }
            }
        }
    }

    public static void validate(Variable var) {
        if (var.isThing()) validate(var.asThing());
    }

    private static void validate(ThingVariable var) {
        if (var.iid().isPresent()) {
            throw TypeDBException.of(THING_IID_NOT_INSERTABLE, var.reference(), var.iid().get());
        } else if (!var.is().isEmpty()) {
            throw TypeDBException.of(ILLEGAL_IS_CONSTRAINT, var, var.is().iterator().next());
        }
    }

    public void execute() {
        List<? extends ConceptMap> matches = getter.execute(context).toList();
        matches.forEach(matched -> new Operation(matched, variables).executeInPlace());
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

        void executeInPlace() {
            variables.forEach(this::delete);
            variables.forEach(this::deleteIsa);
        }

        private void delete(ThingVariable var) {
            Thing thing = matched.get(var.reference().asName()).asThing();
            if (!var.has().isEmpty()) deleteHas(var, thing);
            if (var.relation().isPresent()) deleteRelation(var, thing.asRelation());
            detached.put(var, thing);
        }

        private void deleteHas(ThingVariable var, Thing thing) {
            for (HasConstraint hasConstraint : var.has()) {
                Reference.Name attRef = hasConstraint.attribute().reference().asName();
                Attribute att = matched.get(attRef).asAttribute();
                if (thing.getType().getOwns(att.getType()).isEmpty()) {
                    throw TypeDBException.of(HAS_TYPE_MISMATCH, thing.getType().getLabel(), att.getType().getLabel());
                }
                if (thing.getHas(att.getType()).anyMatch(a -> a.equals(att))) thing.unsetHas(att);
            }
        }

        private void deleteRelation(ThingVariable var, Relation relation) {
            if (var.relation().isPresent()) {
                var.relation().get().players().forEach(rolePlayer -> {
                    Thing player = matched.get(rolePlayer.player().reference().asName()).asThing();
                    RoleType roleType;
                    if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().id().isName()) {
                        Type type = matched.get(rolePlayer.roleType().get().id().asRetrievable()).asType();
                        if (!type.isRoleType()) throw TypeDBException.of(ROLE_TYPE_MISMATCH, type.getLabel());
                        else roleType = type.asRoleType();
                    } else {
                        roleType = tryInferRoleType(relation, player, rolePlayer);
                    }
                    if (relation.getType().getRelates(roleType.getLabel().name()) == null) {
                        throw TypeDBException.of(RELATING_TYPE_MISMATCH, relation.getType().getLabel(), roleType.getLabel());
                    } else if (!player.getType().plays(roleType)) {
                        throw TypeDBException.of(PLAYING_TYPE_MISMATCH, player.getType().getLabel(), roleType.getLabel());
                    }
                    if (relation.getPlayers(roleType).anyMatch(t -> t.equals(player))) {
                        relation.removePlayer(roleType, player);
                    }
                });
            } else {
                throw TypeDBException.of(DELETE_RELATION_CONSTRAINT_TOO_MANY, var.reference());
            }
        }

        private void deleteIsa(ThingVariable var) {
            Thing thing = detached.get(var);
            ThingType type = thing.getType();
            if (var.isa().isPresent() && !thing.isDeleted()) {
                Label typeLabel;
                if (var.isa().get().type().id().isName()) {
                    typeLabel = matched.get(var.isa().get().type().id().asRetrievable()).asType().getLabel();
                } else {
                    typeLabel = var.isa().get().type().label().get().properLabel();
                }
                if (var.isa().get().isExplicit()) {
                    if (type.getLabel().equals(typeLabel)) thing.delete();
                    else throw TypeDBException.of(INVALID_DELETE_THING_DIRECT, var.reference(), typeLabel);
                } else {
                    if (type.getSupertypesWithThing().anyMatch(t -> t.getLabel().equals(typeLabel))) thing.delete();
                    else throw TypeDBException.of(INVALID_DELETE_THING, var.reference(), typeLabel);
                }
                matched.concepts().remove(var.id());
            }
        }
    }
}
