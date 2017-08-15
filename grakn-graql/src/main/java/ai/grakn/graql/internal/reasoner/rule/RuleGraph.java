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

    /**
     * @return set of inference rule contained in the graph
     */
    public static Stream<Rule> getRules(GraknGraph graph) {
        return graph.admin().getMetaRuleInference().instances();
    }

    /**
     * @return true if at least one inference rule is present in the graph
     */
    public static boolean hasRules(GraknGraph graph) {
        Label inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getLabel();
        return graph.graql().infer(false).match(var("x").isa(Graql.label(inferenceRule))).ask().execute();
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    public static Stream<Rule> getRulesWithType(OntologyConcept type, GraknGraph graph){
        return type != null ?
                type.subs().flatMap(OntologyConcept::getRulesOfConclusion) :
                getRules(graph);
    }

    /**
     * @param rules set of rules of interest forming a rule subgraph
     * @return true if the rule subgraph formed from provided rules contains loops with negative net flux (appears in more rule heads than bodies)
     */
    public static boolean subGraphHasLoopsWithNegativeFlux(Set<InferenceRule> rules, GraknGraph graph){
        return rules.stream()
                .map(r -> graph.<Rule>getConcept(r.getRuleId()))
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
     * @param rules set of rules of interest forming a rule subgraph
     * @return true if the rule subgraph formed from provided rules contains any rule with head satisfying the body pattern
     */
    public static boolean subGraphHasRulesWithHeadSatisfyingBody(Set<InferenceRule> rules, GraknGraph graph){
        return rules.stream()
                .filter(InferenceRule::headSatisfiesBody)
                .findFirst().isPresent();
    }

    /**
     * @param topTypes entry types in the rule graph
     * @return all rules that are reachable from the entry types
     */
    public static Stream<Rule> getDependentRules(Set<Type> topTypes){
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

    /**
     * @param query top query
     * @return all rules that are reachable from the entry types
     */
    public static Stream<InferenceRule> getDependentRules(ReasonerQueryImpl query){
        Set<InferenceRule> rules = new HashSet<>();
        Set<Atom> visitedAtoms = new HashSet<>();
        Stack<Atom> atoms = new Stack<>();
        query.selectAtoms().forEach(atoms::push);
        while(!atoms.isEmpty()) {
            Atom atom = atoms.pop();
            if (!visitedAtoms.contains(atom)){
                atom.getApplicableRules()
                        .peek(rules::add)
                        .flatMap(rule -> rule.getBody().selectAtoms().stream())
                        .filter(visitedAtoms::contains)
                        .forEach(atoms::add);
                visitedAtoms.add(atom);
            }
        }
        return rules.stream();
    }
}
