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

package grakn.core.test.behaviour.resolution.framework.resolve;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.graql.reasoner.explanation.RuleExplanation;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.common.ConjunctionFlatteningVisitor;
import grakn.core.test.behaviour.resolution.framework.common.KeyStatementsGenerator;
import grakn.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import grakn.core.test.behaviour.resolution.framework.common.StatementVisitor;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.IdProperty;
import graql.lang.property.NeqProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.VarProperty;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;

public class ResolutionQueryBuilder {

    private RuleResolutionBuilder ruleResolutionBuilder = new RuleResolutionBuilder();

    public List<GraqlGet> buildMatchGet(Transaction tx, GraqlGet query) {
        List<ConceptMap> answers = tx.execute(query, true, true);

        ArrayList<GraqlGet> resolutionQueries = new ArrayList<>();
        for (ConceptMap answer : answers) {
            ConjunctionFlatteningVisitor flattener = new ConjunctionFlatteningVisitor();
            resolutionQueries.add(Graql.match(flattener.visitPattern(resolutionPattern(tx, answer, 0))).get());
        }
        return resolutionQueries;
    }

    private Conjunction<Pattern> resolutionPattern(Transaction tx, ConceptMap answer, Integer ruleResolutionIndex) {

        Pattern answerPattern = answer.getPattern();
        LinkedHashSet<Pattern> resolutionPatterns = new LinkedHashSet<>();

        if (answerPattern == null) {
            throw new RuntimeException("Answer is missing a pattern. Either patterns are broken or the initial query did not require inference.");
        }
        Integer finalRuleResolutionIndex1 = ruleResolutionIndex;

        StatementVisitor statementVisitor = new StatementVisitor(p -> {
            Statement withoutIds = removeIdProperties(makeAnonVarsExplicit(p));
            return withoutIds == null ? null : prefixVars(withoutIds, finalRuleResolutionIndex1);
        });

        resolutionPatterns.add(statementVisitor.visitPattern(answerPattern));

        KeyStatementsGenerator.generateKeyStatements(answer.map()).forEach(statement -> resolutionPatterns.add(prefixVars(statement, finalRuleResolutionIndex1)));

        if (answer.explanation() != null) {

            Explanation explanation = answer.explanation();

            if (explanation.isRuleExplanation()) {

                ConceptMap explAns = getOnlyElement(explanation.getAnswers());

                ruleResolutionIndex += 1;
                Integer finalRuleResolutionIndex0 = ruleResolutionIndex;

                StatementVisitor ruleStatementVisitor = new StatementVisitor(p -> prefixVars(makeAnonVarsExplicit(p), finalRuleResolutionIndex0));

                Pattern whenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().when());
                whenPattern = ruleStatementVisitor.visitPattern(whenPattern);
                resolutionPatterns.add(whenPattern);

                Pattern thenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().then());
                thenPattern = ruleStatementVisitor.visitPattern(thenPattern);
                resolutionPatterns.add(thenPattern);

                String ruleLabel = ((RuleExplanation)explanation).getRule().label().toString();
                resolutionPatterns.add(ruleResolutionBuilder.ruleResolutionConjunction(whenPattern, thenPattern, ruleLabel));
                resolutionPatterns.add(resolutionPattern(tx, explAns, ruleResolutionIndex));
            } else {
                for (ConceptMap explAns : explanation.getAnswers()) {
                    resolutionPatterns.addAll(resolutionPattern(tx, explAns, ruleResolutionIndex).getPatterns());
                }
            }
        }
        return Graql.and(resolutionPatterns);
    }

    public static Statement makeAnonVarsExplicit(Statement statement) {
        if (statement.var().isReturned()) {
            return statement;
        } else {
            return Statement.create(statement.var().asReturnedVar(), statement.properties());
        }
    }

    private Statement prefixVars(Statement statement, Integer ruleResolutionIndex) {
        String prefix = "r" + ruleResolutionIndex + "-";
        String newVarName = prefix + statement.var().name();

        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>();
        for (VarProperty prop : statement.properties()) {

            // TODO implement the rest of these replacements
            if (prop instanceof RelationProperty) {

                List<RelationProperty.RolePlayer> roleplayers = ((RelationProperty) prop).relationPlayers();
                List<RelationProperty.RolePlayer> newRps = roleplayers.stream().map(rp -> {

                    String rpVarName = prefix + rp.getPlayer().var().name();
                    Statement newPlayerStatement = new Statement(new Variable(rpVarName));
                    return new RelationProperty.RolePlayer(rp.getRole().orElse(null), newPlayerStatement);
                }).collect(Collectors.toList());

                newProperties.add(new RelationProperty(newRps));
            } else if (prop instanceof HasAttributeProperty) {

                HasAttributeProperty hasProp = (HasAttributeProperty) prop;
                if (hasProp.attribute().var().isVisible()) {
                    // If the attribute has a variable, rather than a value
                    String newAttributeName = prefix + ((HasAttributeProperty) prop).attribute().var().name();
                    newProperties.add(new HasAttributeProperty(hasProp.type(), new Statement(new Variable(newAttributeName))));
                } else {
                    newProperties.add(hasProp);
                }
            } else if (prop instanceof NeqProperty) {
                NeqProperty neqProp = (NeqProperty) prop;
                String newComparedVarName = prefix + neqProp.statement().var().name();
                newProperties.add(new NeqProperty(Graql.var(newComparedVarName)));
            } else {
                newProperties.add(prop);
            }
        }
        return Statement.create(new Variable(newVarName), newProperties);
    }

    /**
     * Remove properties that stipulate ConceptIds from a given statement
     * @param statement statement to remove from
     * @return statement without any ID properties, null if an ID property was the only property
     */
    public static Statement removeIdProperties(Statement statement) {
        LinkedHashSet<VarProperty> propertiesWithoutIds = new LinkedHashSet<>();
        statement.properties().forEach(varProperty -> {
            if (!(varProperty instanceof IdProperty)) {
                propertiesWithoutIds.add(varProperty);
            }
        });
        if (propertiesWithoutIds.isEmpty()) {
            return null;
        }
        return Statement.create(statement.var(), propertiesWithoutIds);
    }

}
