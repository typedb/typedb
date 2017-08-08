/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.rule;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.util.Schema;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;

/**
 *
 * <p>
 * Convenience class providing methods for operating with the rule graph.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RuleGraph {

    private final GraknGraph graph;

    public RuleGraph(GraknGraph graph){
        this.graph = graph;
    }

    /**
     * @return set of inference rule contained in the graph
     */
    public Stream<Rule> getRules() {
        return graph.admin().getMetaRuleInference().instances();
    }

    /**
     * @return true if at least one inference rule is present in the graph
     */
    public boolean hasRules() {
        Label inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getLabel();
        return graph.graql().infer(false).match(var("x").isa(Graql.label(inferenceRule))).ask().execute();
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    public Stream<Rule> getRulesWithType(OntologyConcept type){
        return type != null ?
                type.subs().flatMap(OntologyConcept::getRulesOfConclusion) :
                getRules();
    }

    /**
     * @return true if the rule graph contains types with negative net flux (appears in more rule heads than bodies)
     */
    public boolean hasTypesWithNegativeFlux(Set<Type> topTypes){
        return getDependentRules(topTypes)
                .flatMap(Rule::getConclusionTypes)
                .distinct()
                .filter(type -> {
                    long outflux = type.getRulesOfHypothesis().count();
                    long influx = type.getRulesOfConclusion().count();
                    return outflux > 0 && influx > outflux;
                })
                .findFirst().isPresent();
    }

    /**
     * @return true if the rule graph contains rules generating fresh variables (occurring in rule head but not in the body)
     */
    public boolean hasRulesGeneratingFreshVariables(Set<Type> topTypes){
        return getDependentRules(topTypes)
                .map(r -> new InferenceRule(r, graph))
                .filter(InferenceRule::generatesFreshVariables)
                .findFirst().isPresent();
    }

    public boolean hasRulesGeneratingFreshVariables(ReasonerQueryImpl query){
        return getDependentRules(query)
                .filter(InferenceRule::generatesFreshVariables)
                .findFirst().isPresent();
    }

    public boolean hasRulesWithEquivalentHeadAndBody(ReasonerQueryImpl query){
        return getDependentRules(query)
                .filter(InferenceRule::isHeadEquivalentToBody)
                .findFirst().isPresent();
    }


    /**
     * @param topTypes entry types in the rule graph
     * @return all rules that are reachable from the entry types
     */
    private Stream<Rule> getDependentRules(Set<Type> topTypes){
        Set<Rule> rules = new HashSet<>();
        Set<Type> visitedTypes = new HashSet<>();
        Stack<Type> types = new Stack<>();
        topTypes.forEach(types::push);
        while(!types.isEmpty()) {
            Type type = types.pop();
            if (!visitedTypes.contains(type)){
                type.getRulesOfConclusion()
                        .peek(rules::add)
                        .flatMap(Rule::getHypothesisTypes)
                        .filter(visitedTypes::contains)
                        .forEach(types::add);
                visitedTypes.add(type);
            }
        }
        return rules.stream();
    }

    private Stream<InferenceRule> getDependentRules(ReasonerQueryImpl query){
        Set<InferenceRule> rules = new HashSet<>();
        Set<Atom> visitedAtoms = new HashSet<>();
        Stack<Atom> atoms = new Stack<>();
        query.selectAtoms().forEach(atoms::push);
        while(!atoms.isEmpty()) {
            Atom atom = atoms.pop();
            if (!visitedAtoms.contains(atom)){
                atom.getApplicableRules()
                        .peek(rules::add)
                        .map(rule -> rule.rewriteToUserDefined(atom))
                        .flatMap(rule -> rule.getBody().selectAtoms().stream())
                        .filter(visitedAtoms::contains)
                        .forEach(atoms::add);
                visitedAtoms.add(atom);
            }
        }
        return rules.stream();
    }
}
