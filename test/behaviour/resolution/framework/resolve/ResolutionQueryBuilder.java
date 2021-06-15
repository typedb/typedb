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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor.IIDConstraintThrower;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor.VariableVisitor;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.VarNameGenerator;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Disjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
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

    private final RuleResolutionBuilder ruleResolutionBuilder;
    private final VarNameGenerator varNameGenerator;
    private final PatternVisitor.Deanonymiser deanonymiser;
    private Map<Concept, List<Retrievable>> varsForIds;
    private Map<String, String> replacementVars;

    public ResolutionQueryBuilder() {
        this.ruleResolutionBuilder = new RuleResolutionBuilder();
        this.varNameGenerator = new VarNameGenerator();
        this.deanonymiser = PatternVisitor.Deanonymiser.create(varNameGenerator);
    }

    public List<TypeQLMatch> buildMatch(Transaction tx, TypeQLMatch query) {
        IIDConstraintThrower.create().visitDisjunction(query.conjunction().normalise());

        List<ConceptMap> answers = tx.query().match(query).toList();
        ArrayList<TypeQLMatch> resolutionQueries = new ArrayList<>();
        for (ConceptMap answer : answers) {
            varsForIds = new HashMap<>();
            replacementVars = new HashMap<>();
            final LinkedHashSet<Pattern> resolutionPatterns = buildResolutionPattern(tx, answer, 0);
            final LinkedHashSet<Pattern> replacedResolutionPatterns = new LinkedHashSet<>();
            for (Pattern p : resolutionPatterns) {
                VariableVisitor sv = VariableVisitor.from(this::deduplicateVars);
                Pattern rp = sv.visitPattern(p);
                replacedResolutionPatterns.add(rp);
            }
            final Conjunction<Pattern> conjunction = TypeQL.and(replacedResolutionPatterns);
            resolutionQueries.add(TypeQL.match(conjunction));
        }
        return resolutionQueries;
    }

    private LinkedHashSet<Pattern> buildResolutionPattern(Transaction tx, ConceptMap answer, Integer ruleResolutionIndex) {
        FunctionalIterator<ConceptMap.Explainable> explainableIterator = answer.explainables().iterator();
        if (!explainableIterator.hasNext()) {
            // TODO: This will throw at the bottom of the recursion. It's actually the termination condition.
            throw new RuntimeException("There were no explainables present in the answer");
        }
        ConceptMap.Explainable explainable = explainableIterator.toList().get(0);
        FunctionalIterator<Explanation> explanations = tx.query().explain(explainable.id());
        Explanation explanation = explanations.toList().get(0);
        Map<Retrievable, Set<Retrievable>> varMapping = explanation.variableMapping();
        // TODO: We could save this work by looking the normalised rule up as we already know them from the completion step
        Disjunction<Conjunction<Conjunctable>> when = explanation.rule().getWhenPreNormalised().normalise();
        when = deanonymiser.visitDisjunction(when);
        ThingVariable<?> then = explanation.rule().getThenPreNormalised();
        then = deanonymiser.deanonymiseIfAnon(then);




        LinkedHashSet<Pattern> resolutionPatterns = new LinkedHashSet<>();

        Integer finalRuleResolutionIndex1 = ruleResolutionIndex;

        VariableVisitor variableVisitor = VariableVisitor.from(p -> {
            Variable withoutIds = removeIdProperties(varNameGenerator.deanonymiseIfAnon(p));
            return withoutIds == null ? null : prefixVars(withoutIds, finalRuleResolutionIndex1);
        });

        resolutionPatterns.add(variableVisitor.visitPattern(answerPattern));

        resolutionPatterns.addAll(generateAttrValueVariables(answer.concepts(), ruleResolutionIndex));

        categoriseVarsByConceptId(answer.concepts(), ruleResolutionIndex);

        if (explanation.isRuleExplanation()) {

            ConceptMap explAns = getOnlyElement(explanation.getAnswers());

            ruleResolutionIndex += 1;
            Integer finalRuleResolutionIndex0 = ruleResolutionIndex;

            VariableVisitor ruleVariableVisitor = VariableVisitor.from(p -> prefixVars(VarNameGenerator.deanonymiseIfAnon(p), finalRuleResolutionIndex0));

            Pattern whenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().when());
            whenPattern = ruleVariableVisitor.visitPattern(whenPattern);
            resolutionPatterns.add(whenPattern);

            Pattern thenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().then());
            thenPattern = ruleVariableVisitor.visitPattern(thenPattern);
            resolutionPatterns.add(thenPattern);

            String ruleLabel = ((RuleExplanation)explanation).getRule().label().toString();
            resolutionPatterns.add(ruleResolutionBuilder.ruleResolutionConjunction(whenPattern, thenPattern, ruleLabel));
            resolutionPatterns.add(TypeQL.and(buildResolutionPattern(tx, explAns, ruleResolutionIndex)));
        } else {
            if (explanation.isLookupExplanation()) {
                for (final Variable variable : answer.getPattern().variables()) {
                    if (variable instanceof VariableRelation) {
                        Pattern p = TypeQL.not(prefixVars(VarNameGenerator.deanonymiseIfAnon(TypeQL.var().isa("isa-property"))
                                .rel(variable.var().name())
                                .has("inferred", true), ruleResolutionIndex));
                        resolutionPatterns.add(p);
                        Variable s = TypeQL.var().isa("relation-property")
                                .has("inferred", true);
                        for (Variable v : variable.variables()) {
                            s = s.rel(v.name());
                        }
                        Pattern p2 = TypeQL.not(prefixVars(VarNameGenerator.deanonymiseIfAnon(s), ruleResolutionIndex));
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
     * During resolution we frequently get two variables from distinct statements that actually refer to the same
     * concept. Here, we identify sets of variables, where variables within each set all refer to the same concept,
     * and then construct a mapping that, when applied to all variables in a conjunction, will ensure that any two
     * distinct variables in that conjunction refer to distinct concepts.
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
    static Variable removeIdProperties(Variable variable) {
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

    public static class ResolutionConstraintException extends RuntimeException {
        public ResolutionConstraintException(String message) {
            super(message);
        }
    }
}
