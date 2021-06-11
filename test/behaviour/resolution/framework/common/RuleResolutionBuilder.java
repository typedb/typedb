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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.BODY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.HEAD;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.INSTANCE;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.OWNED;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.OWNER;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.REL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaRole.ROLEPLAYER;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.HAS_ATTRIBUTE_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.INFERRED;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.ISA_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.RELATION_PROPERTY;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.RESOLUTION;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.ROLE_LABEL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.RULE_LABEL;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.complete.SchemaManager.CompletionSchemaType.TYPE_LABEL;

public class RuleResolutionBuilder {

    private HashMap<String, Integer> nextVarIndex = new HashMap<>();

    /**
     * Constructs the TypeDB structure that captures how the result of a rule was inferred
     *
     * @param when `when` of the rule
     * @param then `then` of the rule
     * @param ruleLabel   rule label
     * @return Pattern for the structure that *connects* the variables involved in the rule
     */
    // This implementation for `ruleResolutionConjunction` takes account of disjunctions in rules, however it produces
    // a format for the relation query that seems to be far slower (causing tests to run 2x slower overall), and so it
    // has been put aside for now in favour of an adaptation of the old, simpler, implementation.
//    public Conjunction<? extends Pattern> ruleResolutionConjunction(Pattern whenPattern, Pattern thenPattern, String ruleLabel) {
//        String inferenceType = "resolution";
//        String inferenceRuleLabelType = "rule-label";
//        Variable ruleVar = new Variable(getNextVar("rule"));
//        Variable relation = TypeQL.var(ruleVar).isa(inferenceType).has(inferenceRuleLabelType, ruleLabel);
//        VariableVisitor bodyVisitor = new VariableVisitor(p -> variableToResolutionConjunction(p, ruleVar, "body"));
//        VariableVisitor headVisitor = new VariableVisitor(p -> variableToResolutionConjunction(p, ruleVar, "head"));
//        NegationRemovalVisitor negationStripper = new NegationRemovalVisitor();
//        Pattern body = bodyVisitor.visitPattern(negationStripper.visitPattern(whenPattern));
//        Pattern head = headVisitor.visitPattern(thenPattern);
//        return TypeQL.and(body, head, relation);
//    }
//
//    private Pattern variableToResolutionConjunction(Variable variable, Variable ruleVar, String ruleRole) {
//        LinkedHashMap<String, Variable> resolutionProperties = variableToResolutionProperties(variable);
//        if (resolutionProperties.isEmpty()) {
//            return null;
//        } else {
//            LinkedHashSet<Variable> s = new LinkedHashSet<>();
//            Variable ruleVariable = TypeQL.var(ruleVar);
//            for (String var : resolutionProperties.keySet()) {
//                ruleVariable = ruleVariable.rel(ruleRole, TypeQL.var(var));
//            }
//            s.add(ruleVariable);
//            s.addAll(resolutionProperties.values());
//            return TypeQL.and(s);
//        }
//    }

    // This implementation doesn't handle disjunctions in rules.
    public Conjunction<? extends Pattern> ruleResolutionConjunction(Transaction tx, com.vaticle.typedb.core.pattern.Conjunction when, com.vaticle.typedb.core.pattern.Conjunction then, String ruleLabel) {

        PatternVisitor.NegationRemovalVisitor negationStripper = new PatternVisitor.NegationRemovalVisitor();
        com.vaticle.typedb.core.pattern.Conjunction strippedWhen = negationStripper.visitConjunction(when);
        com.vaticle.typedb.core.pattern.Conjunction strippedThen = negationStripper.visitConjunction(then);

        UnboundVariable relationVar = TypeQL.var(REL.toString());
        List<ThingVariable<?>> constraints = new ArrayList<>();
        constraints.add(relationVar.isa(RESOLUTION.toString()).has(RULE_LABEL.toString(), ruleLabel));

        LinkedHashMap<String, ThingVariable<?>> whenProps = new LinkedHashMap<>();

        for (Variable whenVariable : strippedWhen.variables()) {
            whenProps.putAll(addTrackingConstraints(tx, whenVariable, null));
        }

        for (String whenVar : whenProps.keySet()) {
            constraints.add(relationVar.rel(BODY.toString(), whenVar));
        }

        LinkedHashMap<String, ThingVariable<?>> thenProps = new LinkedHashMap<>();

        for (Variable thenVariable : strippedThen.variables()) {
            thenProps.putAll(addTrackingConstraints(tx, thenVariable, true));
        }

        for (String thenVar : thenProps.keySet()) {
            constraints.add(relationVar.rel(HEAD.toString(), TypeQL.var(thenVar)));
        }

        Conjunction<ThingVariable<?>> conjunction = new Conjunction<>(constraints);

        com.vaticle.typedb.core.pattern.Conjunction.create(conjunction.normalise().patterns().get(0));
        LinkedHashSet<Variable> result = new LinkedHashSet<>();
        result.addAll(whenProps.values());
        result.addAll(thenProps.values());
        result.add(relation);
        return TypeQL.and(result);
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

    public LinkedHashMap<String, ThingVariable<?>> addTrackingConstraints(final Transaction tx, Variable variable, @Nullable final Boolean inferred) {
        LinkedHashMap<String, ThingVariable<?>> newVariables = new LinkedHashMap<>();

        String variableVarName = variable.toString();

        String nextVarName = getNextVarName(VarPrefix.X.toString());
        ThingVariable.Relation isaRelation = TypeQL.var(nextVarName)
                .rel(INSTANCE.toString(), variableVarName)
                .isa(ISA_PROPERTY.toString());
        for (Label typeLabel : variable.resolvedTypes()) {
            isaRelation = isaRelation.has(TYPE_LABEL.toString(), typeLabel.name());
        }
        if (inferred != null) isaRelation = isaRelation.has(INFERRED.toString(), inferred); // TODO: Remove null check
        newVariables.put(nextVarName, isaRelation);

        for (Constraint constraint : variable.constraints()) {
            if (constraint.isThing()) {
                if (constraint.asThing().isHas()) {
                    nextVarName = getNextVarName(VarPrefix.X.toString());
                    ThingVariable.Relation relation = TypeQL.var(nextVarName)
                            .rel(OWNED.toString(), constraint.asThing().asHas().attribute().toString())
                            .rel(OWNER.toString(), variableVarName)
                            .isa(HAS_ATTRIBUTE_PROPERTY.toString());
                    if (inferred != null) relation = relation.has(INFERRED.toString(), inferred); // TODO: Remove null check
                    newVariables.put(nextVarName, relation);
                } else if (constraint.asThing().isRelation()) {
                    for (RelationConstraint.RolePlayer rolePlayer : constraint.asThing().asRelation().players()) {
                        nextVarName = getNextVarName(VarPrefix.X.toString());
                        ThingVariable.Relation relation = TypeQL.var(nextVarName)
                                .rel(REL.toString(), variableVarName)
                                .rel(ROLEPLAYER.toString(), TypeQL.var(rolePlayer.player().toString()))
                                .isa(RELATION_PROPERTY.toString());
                        Optional<TypeVariable> role = rolePlayer.roleType();
                        if (role.isPresent()) {
                            for (Label roleLabel : role.get().resolvedTypes()) {
                                relation = relation.has(ROLE_LABEL.toString(), roleLabel.toString());
                            }
                        }
                        if (inferred != null) relation = relation.has(INFERRED.toString(), inferred); // TODO: Remove null check
                        newVariables.put(nextVarName, relation);
                    }
                }
            }
        }
        return newVariables;
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
