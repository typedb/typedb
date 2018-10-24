/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.graql.internal.reasoner.rule;

import ai.grakn.GraknTx;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicEquivalence;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import com.google.common.base.Equivalence;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Convenience class providing methods for operating with the rule graph.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RuleUtils {

    /**
     * @param graph of interest
     * @return set of inference rule contained in the graph
     */
    public static Stream<Rule> getRules(GraknTx graph) {
        return ((EmbeddedGraknTx<?>) graph).ruleCache().getRules();
    }

    /**
     * @param graph of interest
     * @return true if at least one inference rule is present in the graph
     */
    public static boolean hasRules(GraknTx graph) {
        return graph.admin().getMetaRule().subs().anyMatch(rule -> !rule.label().equals(Schema.MetaSchema.RULE.getLabel()));
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @param graph of interest
     * @return rules containing specified type in the head
     */
    public static Stream<Rule> getRulesWithType(SchemaConcept type, boolean direct, GraknTx graph){
        return ((EmbeddedGraknTx<?>) graph).ruleCache().getRulesWithType(type, direct);
    }

    /**
     * @param rules set of rules of interest forming a rule subgraph
     * @return true if the rule subgraph formed from provided rules contains loops
     */
    public static boolean subGraphIsCyclical(Set<InferenceRule> rules){
        Iterator<Rule> ruleIterator = rules.stream()
                .map(InferenceRule::getRule)
                .iterator();
        boolean cyclical = false;
        while (ruleIterator.hasNext() && !cyclical){
            Set<Rule> visitedRules = new HashSet<>();
            Stack<Rule> rulesToVisit = new Stack<>();
            rulesToVisit.push(ruleIterator.next());
            while(!rulesToVisit.isEmpty() && !cyclical) {
                Rule rule = rulesToVisit.pop();
                if (!visitedRules.contains(rule)){
                    rule.thenTypes()
                            .flatMap(SchemaConcept::whenRules)
                            .forEach(rulesToVisit::add);
                    visitedRules.add(rule);
                } else {
                    cyclical = true;
                }
            }
        }
        return cyclical;
    }

    /**
     * @param rules set of rules of interest forming a rule subgraph
     * @return true if the rule subgraph formed from provided rules contains any rule with head satisfying the body pattern
     */
    public static boolean subGraphHasRulesWithHeadSatisfyingBody(Set<InferenceRule> rules){
        return rules.stream()
                .anyMatch(InferenceRule::headSatisfiesBody);
    }

    /**
     * @param query top query
     * @return all rules that are reachable from the entry types
     */
    public static Set<InferenceRule> getDependentRules(ReasonerQueryImpl query){
        final AtomicEquivalence equivalence = AtomicEquivalence.AlphaEquivalence;

        Set<InferenceRule> rules = new HashSet<>();
        Set<Equivalence.Wrapper<Atom>> visitedAtoms = new HashSet<>();
        Stack<Equivalence.Wrapper<Atom>> atoms = new Stack<>();
        query.selectAtoms().map(equivalence::wrap).forEach(atoms::push);
        while(!atoms.isEmpty()) {
            Equivalence.Wrapper<Atom> wrappedAtom = atoms.pop();
             Atom atom = wrappedAtom.get();
            if (!visitedAtoms.contains(wrappedAtom) && atom != null){
                atom.getApplicableRules()
                        .peek(rules::add)
                        .flatMap(rule -> rule.getBody().selectAtoms())
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
