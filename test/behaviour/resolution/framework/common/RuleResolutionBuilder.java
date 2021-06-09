/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.test.behaviour.resolution.framework.common;

import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Transaction;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;

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
     * Constructs the Grakn structure that captures how the result of a rule was inferred
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
//        Statement relation = Graql.var(ruleVar).isa(inferenceType).has(inferenceRuleLabelType, ruleLabel);
//        StatementVisitor bodyVisitor = new StatementVisitor(p -> statementToResolutionConjunction(p, ruleVar, "body"));
//        StatementVisitor headVisitor = new StatementVisitor(p -> statementToResolutionConjunction(p, ruleVar, "head"));
//        NegationRemovalVisitor negationStripper = new NegationRemovalVisitor();
//        Pattern body = bodyVisitor.visitPattern(negationStripper.visitPattern(whenPattern));
//        Pattern head = headVisitor.visitPattern(thenPattern);
//        return Graql.and(body, head, relation);
//    }
//
//    private Pattern statementToResolutionConjunction(Statement statement, Variable ruleVar, String ruleRole) {
//        LinkedHashMap<String, Statement> resolutionProperties = statementToResolutionProperties(statement);
//        if (resolutionProperties.isEmpty()) {
//            return null;
//        } else {
//            LinkedHashSet<Statement> s = new LinkedHashSet<>();
//            Statement ruleStatement = Graql.var(ruleVar);
//            for (String var : resolutionProperties.keySet()) {
//                ruleStatement = ruleStatement.rel(ruleRole, Graql.var(var));
//            }
//            s.add(ruleStatement);
//            s.addAll(resolutionProperties.values());
//            return Graql.and(s);
//        }
//    }

    // This implementation doesn't handle disjunctions in rules.
    public Conjunction<? extends Pattern> ruleResolutionConjunction(Transaction tx, Pattern whenPattern, Pattern thenPattern, String ruleLabel) {

        NegationRemovalVisitor negationStripper = new NegationRemovalVisitor();
        Set<Statement> whenStatements = negationStripper.visitPattern(whenPattern).statements();
        Set<Statement> thenStatements = negationStripper.visitPattern(thenPattern).statements();

        String inferenceType = "resolution";
        String inferenceRuleLabelType = "rule-label";

        Statement relation = Graql.var().isa(inferenceType).has(inferenceRuleLabelType, ruleLabel);

        LinkedHashMap<String, Statement> whenProps = new LinkedHashMap<>();

        for (Statement whenStatement : whenStatements) {
            whenProps.putAll(statementToResolutionProperties(tx, whenStatement, null));
        }

        for (String whenVar : whenProps.keySet()) {
            relation = relation.rel("body", Graql.var(whenVar));
        }

        LinkedHashMap<String, Statement> thenProps = new LinkedHashMap<>();

        for (Statement thenStatement : thenStatements) {
            thenProps.putAll(statementToResolutionProperties(tx, thenStatement, true));
        }

        for (String thenVar : thenProps.keySet()) {
            relation = relation.rel("head", Graql.var(thenVar));
        }

        LinkedHashSet<Statement> result = new LinkedHashSet<>();
        result.addAll(whenProps.values());
        result.addAll(thenProps.values());
        result.add(relation);
        return Graql.and(result);
    }

    public LinkedHashMap<String, Statement> statementToResolutionProperties(final Transaction tx, Statement statement, @Nullable final Boolean inferred) {
        LinkedHashMap<String, Statement> props = new LinkedHashMap<>();

        String statementVarName = statement.var().name();

        for (VarProperty varProp : statement.properties()) {

            if (varProp instanceof HasAttributeProperty) {
                String nextVar = getNextVar("x");
                StatementInstance propStatement = Graql.var(nextVar).isa("has-attribute-property")
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

                    StatementInstance propStatement = Graql.var(nextVar).isa("relation-property").rel("rel", statementVarName).rel("roleplayer", Graql.var(rolePlayer.getPlayer().var()));
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
                StatementInstance propStatement = Graql.var(nextVar).isa("isa-property").rel("instance", statementVarName);
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
