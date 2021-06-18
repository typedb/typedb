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
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor.GetVariables;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor.IIDConstraintThrower;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.PatternVisitor.VariableVisitor;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.VarNameGenerator;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.RuleResolutionBuilder;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.VarNameGenerator.VarPrefix;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunctable;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Disjunction;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.Variable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

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

    private LinkedHashSet<Pattern> buildResolutionPattern(Transaction tx, ConceptMap answer,
                                                          Map<String, String> answerNameSubstitutions,
                                                          Integer ruleResolutionIndex) {

        // TODO: Open questions
        //  1. how do we both deanonymise and keep track of the new names of the anonymous variables.
        //  2. what map of variables do we use for mapping? all variables in the when and then? Separate maps for each?
        FunctionalIterator<ConceptMap.Explainable> explainableIterator = answer.explainables().iterator();
        if (!explainableIterator.hasNext()) {
            // TODO: This will throw at the bottom of the recursion. It's actually the termination condition.
            throw new RuntimeException("There were no explainables present in the answer");
        }
        LinkedHashSet<Pattern> resolutionPatterns = new LinkedHashSet<>();
        for (ConceptMap.Explainable explainable : explainableIterator.toList()) {
            FunctionalIterator<Explanation> explanations = tx.query().explain(explainable.id());
            Explanation explanation = explanations.toList().get(0); // TODO: Use more than just the first explanation
            Map<Retrievable, Set<Retrievable>> unifierMapping = explanation.variableMapping();
            Map<String, Set<String>> unifierMappingStr = convertToStringVarNames(unifierMapping);
            Map<String, Set<String>> substituted = substituteVarNames(unifierMappingStr, answerNameSubstitutions);
            Map<String, String> reversed = reverse(substituted);

            // TODO: We could save this work by looking up the normalised rule as we already know them from the completion step
            Disjunction<Conjunction<Conjunctable>> when = explanation.rule().getWhenPreNormalised().normalise();
            when = deanonymiser.visitDisjunction(when);

            // TODO: Too arduous to get all variables from a disjunction
            GetVariables variablesGetter = GetVariables.create();
            variablesGetter.visitDisjunction(when);
            Map<String, String> whenMapping = fullMapping(reversed, variablesGetter.variables());

            Disjunction<Conjunction<Conjunctable>> mappedWhen = when.remap(whenMapping);
            ThingVariable<?> then = deanonymiser.deanonymiseIfAnon(explanation.rule().getThenPreNormalised());
            Map<String, String> thenMapping = fullMapping(reversed, then.variables().collect(Collectors.toSet())); // TODO: Check .variables() gets all of the variables present
            ThingVariable<?> mappedThen = then.remap(thenMapping);

            resolutionPatterns.addAll(generateAttrValueVariables(answer.concepts(), ruleResolutionIndex));
            categoriseVarsByConceptId(answer.concepts(), ruleResolutionIndex);

            ruleResolutionIndex += 1;

            // De-anonymise the when and rename the variables to avoid collisions
            resolutionPatterns.add(mappedWhen);
            // De-anonymise the then and rename the variables to avoid collisions
            resolutionPatterns.add(mappedThen);

            // TODO: Extracting the Conjunction from the normalised `when` is awkward
            assert mappedWhen.patterns().size() == 1;
            resolutionPatterns.add(ruleResolutionBuilder.ruleResolutionConjunction(mappedWhen.patterns().get(0), mappedThen, explanation.rule().getLabel()));
            resolutionPatterns.addAll(buildResolutionPattern(tx, explanation.conditionAnswer(), whenMapping, ruleResolutionIndex));

            // If there is a lookup explanation, and there is a variable that's a variable relation:
            // Add `not { ($r) isa isa-property, has inferred true; };`
            // Add `($x, $y, ...) isa relation-property, has inferred true; };` where $x, $y etc were the roleplayers of $r
        }
        return resolutionPatterns;
    }

    private Map<String, Set<String>> convertToStringVarNames(Map<Retrievable, Set<Retrievable>> unifierMapping) {
        Map<String, Set<String>> stringUnifierMapping = new HashMap<>();
        unifierMapping.forEach((from, to) -> stringUnifierMapping.put(from.name(), iterate(to).map(Retrievable::name).toSet()));
        return stringUnifierMapping;
    }

    private Map<String, String> fullMapping(Map<String, String> varMapping, Set<BoundVariable> variables) {
        Map<String, String> fullMapping = new HashMap<>(varMapping);
        iterate(variables).filter(Variable::isNamed).map(var -> var.reference().name())
                .forEachRemaining(n -> fullMapping.putIfAbsent(n, varNameGenerator.getNextVarName(VarPrefix.X)));
        return fullMapping;
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

    public static class ResolutionConstraintException extends RuntimeException {
        public ResolutionConstraintException(String message) {
            super(message);
        }
    }
}
