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

import ai.grakn.GraknTx;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.util.Schema;

import com.google.common.base.Equivalence;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
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
public class RuleUtil {

    /**
     * @param graph of interest
     * @return set of inference rule contained in the graph
     */
    public static Stream<Rule> getRules(GraknTx graph) {
        return graph.admin().getMetaRule().subs().
                filter(sub -> !sub.equals(graph.admin().getMetaRule()));
    }

    /**
     * @param graph of interest
     * @return true if at least one inference rule is present in the graph
     */
    public static boolean hasRules(GraknTx graph) {
        VarPattern rule = label(Schema.MetaSchema.RULE.getLabel());
        return graph.graql().infer(false).match(var("x").sub(rule).neq(rule)).iterator().hasNext();
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @param graph of interest
     * @return rules containing specified type in the head
     */
    public static Stream<Rule> getRulesWithType(SchemaConcept type, GraknTx graph){
        return type != null ?
                type.subs().flatMap(SchemaConcept::getRulesOfConclusion) :
                getRules(graph);
    }

    /**
     * @param rules set of rules of interest forming a rule subgraph
     * @param graph of interest
     * @return true if the rule subgraph formed from provided rules contains loops
     */
    public static boolean subGraphHasLoops(Set<InferenceRule> rules, GraknTx graph){
        return rules.stream()
                .map(r -> graph.<Rule>getConcept(r.getRuleId()))
                .flatMap(Rule::getConclusionTypes)
                .distinct()
                .filter(type -> type.getRulesOfHypothesis().findFirst().isPresent())
                .findFirst().isPresent();
    }

    /**
     * @param rules set of rules of interest forming a rule subgraph
     * @param graph of interest
     * @return true if the rule subgraph formed from provided rules contains loops with negative net flux (appears in more rule heads than bodies)
     */
    public static boolean subGraphHasLoopsWithNegativeFlux(Set<InferenceRule> rules, GraknTx graph){
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
    public static boolean subGraphHasRulesWithHeadSatisfyingBody(Set<InferenceRule> rules){
        return rules.stream()
                .filter(InferenceRule::headSatisfiesBody)
                .findFirst().isPresent();
    }

    /**
     * @param topTypes entry types in the rule graph
     * @return all rules that are reachable from the entry types
     */
    public static Set<Rule> getDependentRules(Set<Type> topTypes){
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
        return rules;
    }

    /**
     * @param query top query
     * @return all rules that are reachable from the entry types
     */
    public static Set<InferenceRule> getDependentRules(ReasonerQueryImpl query){
        final Equivalence<Atom> equivalence = new Equivalence<Atom>(){
            @Override
            protected boolean doEquivalent(Atom a1, Atom a2) {return a1.isAlphaEquivalent(a2);}
            @Override
            protected int doHash(Atom a) {return a.alphaEquivalenceHashCode();}
        };

        Set<InferenceRule> rules = new HashSet<>();
        Set<Equivalence.Wrapper<Atom>> visitedAtoms = new HashSet<>();
        Stack<Equivalence.Wrapper<Atom>> atoms = new Stack<>();
        query.selectAtoms().stream().map(equivalence::wrap).forEach(atoms::push);
        while(!atoms.isEmpty()) {
            Equivalence.Wrapper<Atom> wrappedAtom = atoms.pop();
             Atom atom = wrappedAtom.get();
            if (!visitedAtoms.contains(wrappedAtom) && atom != null){
                atom.getApplicableRules()
                        .peek(rules::add)
                        .flatMap(rule -> rule.getBody().selectAtoms().stream())
                        .map(equivalence::wrap)
                        .filter(at -> !visitedAtoms.contains(at))
                        .filter(at -> !atoms.contains(at))
                        .forEach(atoms::add);
                visitedAtoms.add(wrappedAtom);
            }
        }
        return rules;
    }
}
