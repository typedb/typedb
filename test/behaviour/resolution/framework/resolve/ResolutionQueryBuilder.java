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
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.common.ConjunctionFlatteningVisitor;
import grakn.core.test.behaviour.resolution.framework.common.GraqlHelpers;
import grakn.core.test.behaviour.resolution.framework.common.ResolutionConstraintException;
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
import graql.lang.statement.StatementAttribute;
import graql.lang.statement.StatementRelation;
import graql.lang.statement.Variable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;

public class ResolutionQueryBuilder {

    private RuleResolutionBuilder ruleResolutionBuilder = new RuleResolutionBuilder();
    private Map<ConceptId, List<String>> varsForIds;
    private Map<String, String> replacementVars;

    public List<GraqlGet> buildMatchGet(Transaction tx, GraqlGet query) {
        List<ConceptMap> answers = tx.execute(query, true, true);

        ArrayList<GraqlGet> resolutionQueries = new ArrayList<>();
        for (ConceptMap answer : answers) {
            varsForIds = new HashMap<>();
            replacementVars = new HashMap<>();
            ConjunctionFlatteningVisitor flattener = new ConjunctionFlatteningVisitor();
            final LinkedHashSet<Pattern> resolutionPatterns = buildResolutionPattern(tx, answer, 0);
            final LinkedHashSet<Pattern> replacedResolutionPatterns = new LinkedHashSet<>();
            for (Pattern p : resolutionPatterns) {
                StatementVisitor sv = new StatementVisitor(this::deduplicateVars);
                Pattern rp = sv.visitPattern(p);
                replacedResolutionPatterns.add(rp);
            }
            final Conjunction<Pattern> conjunction = Graql.and(replacedResolutionPatterns);
            resolutionQueries.add(Graql.match(flattener.visitPattern(conjunction)).get());
        }
        return resolutionQueries;
    }

    private LinkedHashSet<Pattern> buildResolutionPattern(Transaction tx, ConceptMap answer, Integer ruleResolutionIndex) {

        Pattern answerPattern = answer.getPattern();
        LinkedHashSet<Pattern> resolutionPatterns = new LinkedHashSet<>();

        if (answerPattern == null) {
            throw new RuntimeException("Answer is missing a pattern. Either patterns are broken or the initial query did not require inference.");
        }
        Integer finalRuleResolutionIndex1 = ruleResolutionIndex;

        StatementVisitor statementVisitor = new StatementVisitor(p -> {
            Statement withoutIds = removeIdProperties(GraqlHelpers.makeAnonVarsExplicit(p));
            return withoutIds == null ? null : prefixVars(withoutIds, finalRuleResolutionIndex1);
        });

        resolutionPatterns.add(statementVisitor.visitPattern(answerPattern));

        resolutionPatterns.addAll(generateAttrValueStatements(answer.map(), ruleResolutionIndex));

        categoriseVarsByConceptId(answer.map(), ruleResolutionIndex);

        if (answer.explanation() != null) {

            Explanation explanation = answer.explanation();

            if (explanation.isRuleExplanation()) {

                ConceptMap explAns = getOnlyElement(explanation.getAnswers());

                ruleResolutionIndex += 1;
                Integer finalRuleResolutionIndex0 = ruleResolutionIndex;

                StatementVisitor ruleStatementVisitor = new StatementVisitor(p -> prefixVars(GraqlHelpers.makeAnonVarsExplicit(p), finalRuleResolutionIndex0));

                Pattern whenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().when());
                whenPattern = ruleStatementVisitor.visitPattern(whenPattern);
                resolutionPatterns.add(whenPattern);

                Pattern thenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().then());
                thenPattern = ruleStatementVisitor.visitPattern(thenPattern);
                resolutionPatterns.add(thenPattern);

                String ruleLabel = ((RuleExplanation)explanation).getRule().label().toString();
                resolutionPatterns.add(ruleResolutionBuilder.ruleResolutionConjunction(tx, whenPattern, thenPattern, ruleLabel));
                resolutionPatterns.add(Graql.and(buildResolutionPattern(tx, explAns, ruleResolutionIndex)));
            } else {
                if (explanation.isLookupExplanation()) {
                    for (final Statement statement : answer.getPattern().statements()) {
                        if (statement instanceof StatementRelation) {
                            Pattern p = Graql.not(prefixVars(GraqlHelpers.makeAnonVarsExplicit(Graql.var().isa("isa-property"))
                                    .rel(statement.var().name())
                                    .has("inferred", true), ruleResolutionIndex));
                            resolutionPatterns.add(p);
                            Statement s = Graql.var().isa("relation-property")
                                    .has("inferred", true);
                            for (Variable v : statement.variables()) {
                                s = s.rel(v.name());
                            }
                            Pattern p2 = Graql.not(prefixVars(GraqlHelpers.makeAnonVarsExplicit(s), ruleResolutionIndex));
                            resolutionPatterns.add(p2);
                        } /* else {
                            // TODO: support attribute ownerships?
                        } */
                    }
                }
                for (ConceptMap explAns : explanation.getAnswers()) {
                    resolutionPatterns.addAll(buildResolutionPattern(tx, explAns, ruleResolutionIndex));
                }
            }
        }

        return resolutionPatterns;
    }

    /**
     * During resolution, attributes can be easily identified by their value. Adding their value to the resolution
     * query allows us to easily ensure we are querying for the correct attributes.
     */
    private Set<Statement> generateAttrValueStatements(Map<Variable, Concept> varMap, int ruleResolutionIndex) {
        LinkedHashSet<Statement> statements = new LinkedHashSet<>();

        for (Map.Entry<Variable, Concept> entry : varMap.entrySet()) {
            Variable var = entry.getKey();
            Concept concept = entry.getValue();

            if (concept.isAttribute()) {

                String typeLabel = concept.asAttribute().type().label().toString();
                Statement statement = Graql.var(var).isa(typeLabel);
                StatementAttribute s = null;

                Object attrValue = concept.asAttribute().value();
                if (attrValue instanceof String) {
                    s = statement.val((String) attrValue);
                } else if (attrValue instanceof Double) {
                    s = statement.val((Double) attrValue);
                } else if (attrValue instanceof Long) {
                    s = statement.val((Long) attrValue);
                } else if (attrValue instanceof LocalDateTime) {
                    s = statement.val((LocalDateTime) attrValue);
                } else if (attrValue instanceof Boolean) {
                    s = statement.val((Boolean) attrValue);
                }
                statements.add(prefixVars(s, ruleResolutionIndex));
            }
        }
        return statements;
    }

    /**
     * During resolution we frequently get two variables from distinct statements that actually refer to the same
     * concept. Here, we identify sets of variables, where variables within each set all refer to the same concept,
     * and then construct a mapping that, when applied to all variables in a statement, will ensure that any two
     * distinct variables in that statement refer to distinct concepts.
     */
    private void categoriseVarsByConceptId(Map<Variable, Concept> varMap, int ruleResolutionIndex) {
        for (Map.Entry<Variable, Concept> entry : varMap.entrySet()) {
            Variable var = entry.getKey();
            Concept concept = entry.getValue();
            if (concept.isEntity() | concept.isRelation()) {
                final String prefixedVarName = prefixVar(var.name(), ruleResolutionIndex);
                varsForIds.putIfAbsent(concept.id(), new ArrayList<>());
                varsForIds.get(concept.id()).add(prefixedVarName);
            } else if (!concept.isAttribute()) {
                throw new ResolutionConstraintException("Presently we only handle queries concerning Things, not Types");
            }
        }
        for (Map.Entry<ConceptId, List<String>> x : varsForIds.entrySet()) {
            for (String y : x.getValue()) {
                replacementVars.put(y, x.getValue().get(0));
            }
        }
    }

    private String prefixVar(final String varName, final Integer ruleResolutionIndex) {
        return "r" + ruleResolutionIndex + "-" + varName;
    }

    private Statement prefixVars(Statement statement, Integer ruleResolutionIndex) {
        return replaceVars(statement, name -> prefixVar(name, ruleResolutionIndex));
    }

    /**
     * During resolution we frequently get two variables from distinct statements that actually refer to the same
     * concept. Here, we "merge" the variables into having the same variable label, to avoid getting extra answers.
     */
    private Statement deduplicateVars(Statement statement) {
        return replaceVars(statement, name -> replacementVars.getOrDefault(name, name));
    }

    /**
     * Replaces all variables in the given statement according to the specified string replacement function.
     */
    private Statement replaceVars(Statement statement, Function<String, String> nameMapper) {
        String newVarName = nameMapper.apply(statement.var().name());

        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>();
        for (VarProperty prop : statement.properties()) {

            // TODO implement the rest of these replacements
            if (prop instanceof RelationProperty) {

                List<RelationProperty.RolePlayer> roleplayers = ((RelationProperty) prop).relationPlayers();
                List<RelationProperty.RolePlayer> newRps = roleplayers.stream().map(rp -> {

                    String rpVarName = nameMapper.apply(rp.getPlayer().var().name());
                    Statement newPlayerStatement = new Statement(new Variable(rpVarName));
                    return new RelationProperty.RolePlayer(rp.getRole().orElse(null), newPlayerStatement);
                }).collect(Collectors.toList());

                newProperties.add(new RelationProperty(newRps));
            } else if (prop instanceof HasAttributeProperty) {

                HasAttributeProperty hasProp = (HasAttributeProperty) prop;
                if (hasProp.attribute().var().isVisible()) {
                    // If the attribute has a variable, rather than a value
                    String newAttributeName = nameMapper.apply(((HasAttributeProperty) prop).attribute().var().name());
                    newProperties.add(new HasAttributeProperty(hasProp.type(), new Statement(new Variable(newAttributeName))));
                } else {
                    newProperties.add(hasProp);
                }
            } else if (prop instanceof NeqProperty) {
                NeqProperty neqProp = (NeqProperty) prop;
                String newComparedVarName = nameMapper.apply(neqProp.statement().var().name());
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
