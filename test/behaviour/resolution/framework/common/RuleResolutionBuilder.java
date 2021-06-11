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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.pattern.constraint.Constraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.BODY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.HEAD;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.INSTANCE;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.OWNED;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.OWNER;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.REL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaRole.ROLEPLAYER;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.HAS_ATTRIBUTE_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.INFERRED;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.ISA_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.RELATION_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.RESOLUTION;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.ROLE_LABEL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.RULE_LABEL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.CompletionSchema.CompletionSchemaType.TYPE_LABEL;

public class RuleResolutionBuilder {

    private final HashMap<String, Integer> nextVarIndex = new HashMap<>();

    /**
     * Constructs the TypeDB structure that captures how the result of a rule was inferred
     *
     * @param when `when` of the rule
     * @param then `then` of the rule
     * @param ruleLabel   rule label
     * @return Pattern for the structure that *connects* the variables involved in the rule
     */
    public Conjunction<? extends Pattern> ruleResolutionConjunction(Transaction tx,
                                                                    com.vaticle.typedb.core.pattern.Conjunction when,
                                                                    com.vaticle.typedb.core.pattern.Conjunction then,
                                                                    String ruleLabel) {
        PatternVisitor.NegationRemovalVisitor negationStripper = new PatternVisitor.NegationRemovalVisitor();
        com.vaticle.typedb.core.pattern.Conjunction strippedWhen = negationStripper.visitConjunction(when);
        com.vaticle.typedb.core.pattern.Conjunction strippedThen = negationStripper.visitConjunction(then);

        UnboundVariable relationVar = TypeQL.var(REL.toString());
        List<ThingVariable<?>> constraints = new ArrayList<>();
        constraints.add(relationVar.isa(RESOLUTION.toString()).has(RULE_LABEL.toString(), ruleLabel));

        List<ThingVariable<?>> whenVars = new ArrayList<>();
        for (Variable whenVariable : strippedWhen.variables()) {
            whenVars.addAll(addTrackingConstraints(whenVariable, null));
        }
        for (ThingVariable<?> whenVar : whenVars) {
            constraints.add(relationVar.rel(BODY.toString(), whenVar.name()));
        }
        List<ThingVariable<?>> thenVars = new ArrayList<>();
        for (Variable thenVariable : strippedThen.variables()) {
            thenVars.addAll(addTrackingConstraints(thenVariable, true));
        }
        for (ThingVariable<?> thenVar : thenVars) {
            constraints.add(relationVar.rel(HEAD.toString(), thenVar.name()));
        }
        constraints.addAll(whenVars);
        constraints.addAll(thenVars);
        return new Conjunction<>(constraints);
    }

    public List<ThingVariable<?>> addTrackingConstraints(Variable variable, @Nullable final Boolean inferred) {
        List<ThingVariable<?>> newVariables = new ArrayList<>();
        ThingVariable.Relation isaRelation = TypeQL.var(getNextVarName(VarPrefix.X.toString()))
                .rel(INSTANCE.toString(), variable.reference().name())
                .isa(ISA_PROPERTY.toString());
        for (Label typeLabel : variable.resolvedTypes()) {
            isaRelation = isaRelation.has(TYPE_LABEL.toString(), typeLabel.name());
        }
        if (inferred != null) isaRelation = isaRelation.has(INFERRED.toString(), inferred); // TODO: Remove null check
        newVariables.add(isaRelation);

        for (Constraint constraint : variable.constraints()) {
            if (constraint.isThing()) {
                if (constraint.asThing().isHas()) {
                    ThingVariable.Relation relation = TypeQL.var(getNextVarName(VarPrefix.X.toString()))
                            .rel(OWNED.toString(), constraint.asThing().asHas().attribute().reference().name())
                            .rel(OWNER.toString(), variable.reference().name())
                            .isa(HAS_ATTRIBUTE_PROPERTY.toString());
                    if (inferred != null) relation = relation.has(INFERRED.toString(), inferred); // TODO: Remove null check
                    newVariables.add(relation);
                } else if (constraint.asThing().isRelation()) {
                    for (RelationConstraint.RolePlayer rolePlayer : constraint.asThing().asRelation().players()) {
                        ThingVariable.Relation relation = TypeQL.var(getNextVarName(VarPrefix.X.toString()))
                                .rel(REL.toString(), variable.reference().name())
                                .rel(ROLEPLAYER.toString(), TypeQL.var(rolePlayer.player().reference().name()))
                                .isa(RELATION_PROPERTY.toString());
                        Optional<TypeVariable> role = rolePlayer.roleType();
                        if (role.isPresent()) {
                            for (Label roleLabel : role.get().resolvedTypes()) {
                                relation = relation.has(ROLE_LABEL.toString(), roleLabel.toString());
                            }
                        }
                        if (inferred != null) relation = relation.has(INFERRED.toString(), inferred); // TODO: Remove null check
                        newVariables.add(relation);
                    }
                }
            }
        }
        return newVariables;
    }

    enum VarPrefix {
        X("x");

        private final String name;

        VarPrefix(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Creates a new variable by incrementing a value
     * @param prefix The prefix to use to uniquely identify a set of incremented variables, e.g. `x` will give
     *               `x0`, `x1`, `x2`...
     * @return prefix followed by an auto-incremented integer, as a string
     */
    private String getNextVarName(String prefix){
        nextVarIndex.putIfAbsent(prefix, 0);
        int currentIndex = nextVarIndex.get(prefix);
        String nextVar = prefix + currentIndex;
        nextVarIndex.put(prefix, currentIndex + 1);
        return nextVar;
    }
}
