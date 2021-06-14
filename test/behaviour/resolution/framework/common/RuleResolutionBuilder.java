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

import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.constraint.Constraint;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.TypeVariable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
    public Conjunction<BoundVariable> ruleResolutionConjunction(Conjunction<Conjunctable> when,
                                                                ThingVariable<?> then,
                                                                String ruleLabel) {
        PatternVisitor.NegationRemovalVisitor negationStripper = new PatternVisitor.NegationRemovalVisitor();
        Conjunction<BoundVariable> strippedWhen = negationStripper.visitConjunctionVariables(when);
        BoundVariable strippedThen = negationStripper.visitVariable(then);

        UnboundVariable ruleResolutionRelation = TypeQL.var(REL.toString());
        List<BoundVariable> constraints = new ArrayList<>();
        constraints.add(ruleResolutionRelation.isa(RESOLUTION.toString()).has(RULE_LABEL.toString(), ruleLabel));

        constraints.addAll(strippedWhen.patterns());
        for (BoundVariable whenVar : strippedWhen.patterns()) {
            resolutionVariables(whenVar, null).forEach(whenVarResolution -> {
                constraints.add(whenVarResolution);
                constraints.add(ruleResolutionRelation.rel(BODY.toString(), whenVarResolution.name()));
            });
        }

        constraints.add(strippedThen);
        resolutionVariables(strippedThen, true).forEach(thenVarResolution -> {
            constraints.add(thenVarResolution);
            constraints.add(ruleResolutionRelation.rel(HEAD.toString(), thenVarResolution.name()));
        });
        return new Conjunction<>(constraints);
    }

    public List<ThingVariable<?>> resolutionVariables(BoundVariable variable, @Nullable final Boolean inferred) {
        List<ThingVariable<?>> resolutionVars = new ArrayList<>();
        for (Constraint<?> constraint : variable.constraints()) {
            if (constraint.isThing()) {
                if (constraint.asThing().isHas()) {
                    ThingVariable.Relation hasResolutionVar = TypeQL.var(getNextVarName(VarPrefix.X.toString()))
                            .rel(OWNED.toString(), constraint.asThing().asHas().attribute().reference().name())
                            .rel(OWNER.toString(), variable.reference().name())
                            .isa(HAS_ATTRIBUTE_PROPERTY.toString());
                    if (inferred != null) hasResolutionVar = hasResolutionVar.has(INFERRED.toString(), inferred); // TODO: Remove null check
                    resolutionVars.add(hasResolutionVar);
                } else if (constraint.asThing().isRelation()) {
                    for (ThingConstraint.Relation.RolePlayer roleplayer : constraint.asThing().asRelation().players()) {
                        ThingVariable.Relation roleplayerResolutionVar = TypeQL.var(getNextVarName(VarPrefix.X.toString()))
                                .rel(REL.toString(), variable.reference().name())
                                .rel(ROLEPLAYER.toString(), TypeQL.var(roleplayer.player().reference().name()))
                                .isa(RELATION_PROPERTY.toString());
                        Optional<TypeVariable> role = roleplayer.roleType();
                        if (role.isPresent() && role.get().label().isPresent()) {
                            roleplayerResolutionVar = roleplayerResolutionVar
                                    .has(ROLE_LABEL.toString(), role.get().label().get().label());
                        }
                        if (inferred != null) {
                            roleplayerResolutionVar = roleplayerResolutionVar.has(INFERRED.toString(), inferred); // TODO: Remove null check
                        }
                        resolutionVars.add(roleplayerResolutionVar);
                    }
                } else if (constraint.asThing().isIsa() && constraint.asThing().asIsa().type().label().isPresent()) {
                    ThingVariable.Relation isaRelation = TypeQL.var(getNextVarName(VarPrefix.X.toString()))
                            .rel(INSTANCE.toString(), variable.reference().name())
                            .isa(ISA_PROPERTY.toString())
                            .has(TYPE_LABEL.toString(), constraint.asThing().asIsa().type().label().get().label());
                    if (inferred != null) isaRelation = isaRelation.has(INFERRED.toString(), inferred); // TODO: Remove null check
                    resolutionVars.add(isaRelation);
                } else {
                    // TODO: Add all other constraint types
                    throw new NotImplementedException();
                }
            }
        }
        return resolutionVars;
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
