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
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;

public class RuleResolutionBuilder {

    private HashMap<String, Integer> nextVarIndex = new HashMap<>();

    /**
     * Constructs the TypeDB structure that captures how the result of a rule was inferred
     *
     * @param whenPattern `when` of the rule
     * @param thenPattern `then` of the rule
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
//        Statement relation = TypeQL.var(ruleVar).isa(inferenceType).has(inferenceRuleLabelType, ruleLabel);
//        StatementVisitor bodyVisitor = new StatementVisitor(p -> statementToResolutionConjunction(p, ruleVar, "body"));
//        StatementVisitor headVisitor = new StatementVisitor(p -> statementToResolutionConjunction(p, ruleVar, "head"));
//        NegationRemovalVisitor negationStripper = new NegationRemovalVisitor();
//        Pattern body = bodyVisitor.visitPattern(negationStripper.visitPattern(whenPattern));
//        Pattern head = headVisitor.visitPattern(thenPattern);
//        return TypeQL.and(body, head, relation);
//    }
//
//    private Pattern statementToResolutionConjunction(Statement statement, Variable ruleVar, String ruleRole) {
//        LinkedHashMap<String, Statement> resolutionProperties = statementToResolutionProperties(statement);
//        if (resolutionProperties.isEmpty()) {
//            return null;
//        } else {
//            LinkedHashSet<Statement> s = new LinkedHashSet<>();
//            Statement ruleStatement = TypeQL.var(ruleVar);
//            for (String var : resolutionProperties.keySet()) {
//                ruleStatement = ruleStatement.rel(ruleRole, TypeQL.var(var));
//            }
//            s.add(ruleStatement);
//            s.addAll(resolutionProperties.values());
//            return TypeQL.and(s);
//        }
//    }

    // This implementation doesn't handle disjunctions in rules.
    public Conjunction<? extends Pattern> ruleResolutionConjunction(Transaction tx, Pattern whenPattern, Pattern thenPattern, String ruleLabel) {

        NegationRemovalVisitor negationStripper = new NegationRemovalVisitor();
        Set<Statement> whenStatements = negationStripper.visitPattern(whenPattern).statements();
        Set<Statement> thenStatements = negationStripper.visitPattern(thenPattern).statements();

        String inferenceType = "resolution";
        String inferenceRuleLabelType = "rule-label";

        Statement relation = TypeQL.var().isa(inferenceType).has(inferenceRuleLabelType, ruleLabel);

        LinkedHashMap<String, Statement> whenProps = new LinkedHashMap<>();

        for (Statement whenStatement : whenStatements) {
            whenProps.putAll(statementToResolutionProperties(tx, whenStatement, null));
        }

        for (String whenVar : whenProps.keySet()) {
            relation = relation.rel("body", TypeQL.var(whenVar));
        }

        LinkedHashMap<String, Statement> thenProps = new LinkedHashMap<>();

        for (Statement thenStatement : thenStatements) {
            thenProps.putAll(statementToResolutionProperties(tx, thenStatement, true));
        }

        for (String thenVar : thenProps.keySet()) {
            relation = relation.rel("head", TypeQL.var(thenVar));
        }

        LinkedHashSet<Statement> result = new LinkedHashSet<>();
        result.addAll(whenProps.values());
        result.addAll(thenProps.values());
        result.add(relation);
        return TypeQL.and(result);
    }

    public LinkedHashMap<String, Statement> statementToResolutionProperties(final Transaction tx, Statement statement, @Nullable final Boolean inferred) {
        LinkedHashMap<String, Statement> props = new LinkedHashMap<>();

        String statementVarName = statement.var().name();

        for (VarProperty varProp : statement.properties()) {

            if (varProp instanceof HasAttributeProperty) {
                String nextVar = getNextVar("x");
                StatementInstance propStatement = TypeQL.var(nextVar).isa("has-attribute-property")
                        .rel("owned", ((HasAttributeProperty) varProp).attribute().var().name())
                        .rel("owner", statementVarName);
                if (inferred != null) {
                    propStatement = propStatement.has("inferred", inferred);
                }
                props.put(nextVar, propStatement);

            } else if (varProp instanceof RelationProperty) {
                for (RelationProperty.RolePlayer rolePlayer : ((RelationProperty)varProp).relationPlayers()) {
                    Optional<Statement> role = rolePlayer.getRole();

                    String nextVar = getNextVar("x");

                    StatementInstance propStatement = TypeQL.var(nextVar).isa("relation-property").rel("rel", statementVarName).rel("roleplayer", TypeQL.var(rolePlayer.getPlayer().var()));
                    if (role.isPresent()) {
                        String roleLabel = ((TypeProperty) getOnlyElement(role.get().properties())).name();
                        final Set<Role> roles = tx.getRole(roleLabel).sups().collect(Collectors.toSet());
                        for (Role r : roles) {
                            propStatement = propStatement.has("role-label", r.label().getValue());
                        }
                    }
                    if (inferred != null) {
                        propStatement = propStatement.has("inferred", inferred);
                    }
                    props.put(nextVar, propStatement);
                }
            } else if (varProp instanceof IsaProperty){
                String nextVar = getNextVar("x");
                StatementInstance propStatement = TypeQL.var(nextVar).isa("isa-property").rel("instance", statementVarName);
                final Set<Type> types = tx.getType(new Label(varProp.property())).sups().collect(Collectors.toSet());
                for (Type type : types) {
                    propStatement = propStatement.has("type-label", type.label().getValue());
                }
                if (inferred != null) {
                    propStatement = propStatement.has("inferred", inferred);
                }
                props.put(nextVar, propStatement);
            }
        }
        return props;
    }

    /**
     * Creates a new variable by incrementing a value
     * @param prefix The prefix to use to uniquely identify a set of incremented variables, e.g. `x` will give
     *               `x0`, `x1`, `x2`...
     * @return prefix followed by an auto-incremented integer, as a string
     */
    private String getNextVar(String prefix){
        nextVarIndex.putIfAbsent(prefix, 0);
        int currentIndex = nextVarIndex.get(prefix);
        String nextVar = prefix + currentIndex;
        nextVarIndex.put(prefix, currentIndex + 1);
        return nextVar;
    }
}
