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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.resolve;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.TypeQLHelpers;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.ResolutionConstraintException;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typedb.core.pattern.variable.Variable;

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

    public List<TypeQLMatch> buildMatch(Transaction tx, TypeQLMatch query) {
        List<ConceptMap> answers = tx.query().match(query).toList();

        ArrayList<TypeQLMatch> resolutionQueries = new ArrayList<>();
        for (ConceptMap answer : answers) {
            varsForIds = new HashMap<>();
            replacementVars = new HashMap<>();
            PatternVisitor.ConjunctionFlatteningVisitor flattener = new PatternVisitor.ConjunctionFlatteningVisitor();
            final LinkedHashSet<Pattern> resolutionPatterns = buildResolutionPattern(tx, answer, 0);
            final LinkedHashSet<Pattern> replacedResolutionPatterns = new LinkedHashSet<>();
            for (Pattern p : resolutionPatterns) {
                PatternVisitor.VariableVisitor sv = new PatternVisitor.VariableVisitor(this::deduplicateVars);
                Pattern rp = sv.visitPattern(p);
                replacedResolutionPatterns.add(rp);
            }
            final Conjunction<Pattern> conjunction = TypeQL.and(replacedResolutionPatterns);
            resolutionQueries.add(TypeQL.match(flattener.visitPattern(conjunction)));
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

        PatternVisitor.VariableVisitor variableVisitor = new PatternVisitor.VariableVisitor(p -> {
            Variable withoutIds = removeIdProperties(TypeQLHelpers.makeAnonVarsExplicit(p));
            return withoutIds == null ? null : prefixVars(withoutIds, finalRuleResolutionIndex1);
        });

        resolutionPatterns.add(variableVisitor.visitPattern(answerPattern));

        resolutionPatterns.addAll(generateAttrValueVariables(answer.map(), ruleResolutionIndex));

        categoriseVarsByConceptId(answer.map(), ruleResolutionIndex);

        if (answer.explanation() != null) {

            Explanation explanation = answer.explanation();

            if (explanation.isRuleExplanation()) {

                ConceptMap explAns = getOnlyElement(explanation.getAnswers());

                ruleResolutionIndex += 1;
                Integer finalRuleResolutionIndex0 = ruleResolutionIndex;

                PatternVisitor.VariableVisitor ruleVariableVisitor = new PatternVisitor.VariableVisitor(p -> prefixVars(TypeQLHelpers.makeAnonVarsExplicit(p), finalRuleResolutionIndex0));

                Pattern whenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().when());
                whenPattern = ruleVariableVisitor.visitPattern(whenPattern);
                resolutionPatterns.add(whenPattern);

                Pattern thenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().then());
                thenPattern = ruleVariableVisitor.visitPattern(thenPattern);
                resolutionPatterns.add(thenPattern);

                String ruleLabel = ((RuleExplanation)explanation).getRule().label().toString();
                resolutionPatterns.add(ruleResolutionBuilder.ruleResolutionConjunction(tx, whenPattern, thenPattern, ruleLabel));
                resolutionPatterns.add(TypeQL.and(buildResolutionPattern(tx, explAns, ruleResolutionIndex)));
            } else {
                if (explanation.isLookupExplanation()) {
                    for (final Variable variable : answer.getPattern().variables()) {
                        if (variable instanceof VariableRelation) {
                            Pattern p = TypeQL.not(prefixVars(TypeQLHelpers.makeAnonVarsExplicit(TypeQL.var().isa("isa-property"))
                                    .rel(variable.var().name())
                                    .has("inferred", true), ruleResolutionIndex));
                            resolutionPatterns.add(p);
                            Variable s = TypeQL.var().isa("relation-property")
                                    .has("inferred", true);
                            for (Variable v : variable.variables()) {
                                s = s.rel(v.name());
                            }
                            Pattern p2 = TypeQL.not(prefixVars(TypeQLHelpers.makeAnonVarsExplicit(s), ruleResolutionIndex));
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
    private Set<Variable> generateAttrValueVariables(Map<Variable, Concept> varMap, int ruleResolutionIndex) {
        LinkedHashSet<Variable> variables = new LinkedHashSet<>();

        for (Map.Entry<Variable, Concept> entry : varMap.entrySet()) {
            Variable var = entry.getKey();
            Concept concept = entry.getValue();

            if (concept.isAttribute()) {

                String typeLabel = concept.asAttribute().type().label().toString();
                Variable variable = TypeQL.var(var).isa(typeLabel);
                VariableAttribute s = null;

                Object attrValue = concept.asAttribute().value();
                if (attrValue instanceof String) {
                    s = variable.val((String) attrValue);
                } else if (attrValue instanceof Double) {
                    s = variable.val((Double) attrValue);
                } else if (attrValue instanceof Long) {
                    s = variable.val((Long) attrValue);
                } else if (attrValue instanceof LocalDateTime) {
                    s = variable.val((LocalDateTime) attrValue);
                } else if (attrValue instanceof Boolean) {
                    s = variable.val((Boolean) attrValue);
                }
                variables.add(prefixVars(s, ruleResolutionIndex));
            }
        }
        return variables;
    }

    /**
     * During resolution we frequently get two variables from distinct variables that actually refer to the same
     * concept. Here, we identify sets of variables, where variables within each set all refer to the same concept,
     * and then construct a mapping that, when applied to all variables in a variable, will ensure that any two
     * distinct variables in that variable refer to distinct concepts.
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

    private Variable prefixVars(Variable variable, Integer ruleResolutionIndex) {
        return replaceVars(variable, name -> prefixVar(name, ruleResolutionIndex));
    }

    /**
     * During resolution we frequently get two variables from distinct variables that actually refer to the same
     * concept. Here, we "merge" the variables into having the same variable label, to avoid getting extra answers.
     */
    private Variable deduplicateVars(Variable variable) {
        return replaceVars(variable, name -> replacementVars.getOrDefault(name, name));
    }

    /**
     * Replaces all variables in the given variable according to the specified string replacement function.
     */
    private Variable replaceVars(Variable variable, Function<String, String> nameMapper) {
        String newVarName = nameMapper.apply(variable.var().name());

        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>();
        for (VarProperty prop : variable.properties()) {

            // TODO implement the rest of these replacements
            if (prop instanceof RelationProperty) {

                List<RelationProperty.RolePlayer> roleplayers = ((RelationProperty) prop).relationPlayers();
                List<RelationProperty.RolePlayer> newRps = roleplayers.stream().map(rp -> {

                    String rpVarName = nameMapper.apply(rp.getPlayer().var().name());
                    Variable newPlayerVariable = new Variable(new Variable(rpVarName));
                    return new RelationProperty.RolePlayer(rp.getRole().orElse(null), newPlayerVariable);
                }).collect(Collectors.toList());

                newProperties.add(new RelationProperty(newRps));
            } else if (prop instanceof HasAttributeProperty) {

                HasAttributeProperty hasProp = (HasAttributeProperty) prop;
                if (hasProp.attribute().var().isVisible()) {
                    // If the attribute has a variable, rather than a value
                    String newAttributeName = nameMapper.apply(((HasAttributeProperty) prop).attribute().var().name());
                    newProperties.add(new HasAttributeProperty(hasProp.type(), new Variable(new Variable(newAttributeName))));
                } else {
                    newProperties.add(hasProp);
                }
            } else if (prop instanceof NeqProperty) {
                NeqProperty neqProp = (NeqProperty) prop;
                String newComparedVarName = nameMapper.apply(neqProp.variable().var().name());
                newProperties.add(new NeqProperty(TypeQL.var(newComparedVarName)));
            } else {
                newProperties.add(prop);
            }
        }
        return Variable.create(new Variable(newVarName), newProperties);
    }

    /**
     * Remove properties that stipulate ConceptIds from a given variable
     * @param variable variable to remove from
     * @return variable without any ID properties, null if an ID property was the only property
     */
    public static Variable removeIdProperties(Variable variable) {
        LinkedHashSet<VarProperty> propertiesWithoutIds = new LinkedHashSet<>();
        variable.properties().forEach(varProperty -> {
            if (!(varProperty instanceof IdProperty)) {
                propertiesWithoutIds.add(varProperty);
            }
        });
        if (propertiesWithoutIds.isEmpty()) {
            return null;
        }
        return Variable.create(variable.var(), propertiesWithoutIds);
    }

}
