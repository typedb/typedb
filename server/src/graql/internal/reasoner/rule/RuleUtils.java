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

package grakn.core.graql.internal.reasoner.rule;

import com.google.common.base.Equivalence;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.AtomicEquivalence;
import grakn.core.graql.internal.reasoner.query.CompositeQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.internal.reasoner.utils.TarjanSCC;
import grakn.core.graql.query.pattern.Negation;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionOLTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Convenience class providing methods for operating with the rule graph.
 * </p>
 *
 *
 */
public class RuleUtils {

    /**
     * @param graph of interest
     * @return set of inference rule contained in the graph
     */
    public static Stream<Rule> getRules(Transaction graph) {
        return ((TransactionOLTP) graph).ruleCache().getRules();
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @param graph of interest
     * @return rules containing specified type in the head
     */
    public static Stream<Rule> getRulesWithType(SchemaConcept type, boolean direct, Transaction graph){
        return ((TransactionOLTP) graph).ruleCache().getRulesWithType(type, direct);
    }

    private static HashMultimap<Type,Type> typeGraph(Set<Rule> rules){
        HashMultimap<Type, Type> graph = HashMultimap.create();
        rules
                .forEach(rule ->
                        rule.whenTypes()
                                .flatMap(Type::subs)
                                .forEach(whenType ->
                                        rule.thenTypes()
                                                .flatMap(Type::sups)
                                                .forEach(thenType -> graph.put(whenType, thenType))
                        )
                );
        return graph;
    }

    /**
     * @param rule of interest
     * @param tx transaction of interest
     * @return set of negated types in the provided rule
     */
    private static Set<Type> ruleNegativeTypes(Rule rule, TransactionOLTP tx){
        CompositeQuery query = ReasonerQueries.composite(Iterables.getOnlyElement(rule.when().getNegationDNF().getPatterns()), tx);
        return query.getComplementQueries().stream()
                .flatMap(q -> q.getAtoms(Atom.class))
                .map(Atom::getSchemaConcept)
                .filter(Objects::nonNull)
                .filter(Concept::isType)
                .map(Concept::asType)
                .collect(toSet());
    }

    /**
     * @param rules set of rules of interest forming a rule subgraph
     * @return true if the rule subgraph formed from provided rules contains loops
     */
    public static boolean subGraphIsCyclical(Set<InferenceRule> rules){
        return !new TarjanSCC<>(typeGraph(rules.stream().map(InferenceRule::getRule).collect(toSet())))
                .getCycles().isEmpty();
    }

    /**
     *
     * @param rules set of rules of interest forming a rule subgraph
     * @return true if the rule subgraph is stratifiable (doesn't contain cycles with negation)
     */
    public static List<Set<Type>> negativeCycles(Set<Rule> rules, TransactionOLTP tx){
        HashMultimap<Type, Type> typeGraph = typeGraph(rules);
        return new TarjanSCC<>(typeGraph).getCycles().stream()
                .filter(cycle ->
                        cycle.stream().anyMatch(type ->
                                type.whenRules()
                                        .filter(rule -> ruleNegativeTypes(rule, tx).contains(type))
                                        .anyMatch(rule -> !Sets.intersection(cycle, rule.thenTypes().collect(toSet())).isEmpty())
                        )
                ).collect(toList());
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
