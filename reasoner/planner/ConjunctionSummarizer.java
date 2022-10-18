/*
 * Copyright (C) 2022 Vaticle
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
package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.variable.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConjunctionSummarizer {
    private final LogicManager logicMgr;
    private final Map<ResolvableConjunction, ConjunctionSummary> conjunctionSummaries;

    public ConjunctionSummarizer(LogicManager logicMgr) {
        this.logicMgr = logicMgr;
        this.conjunctionSummaries = new HashMap<>();
    }

    public Set<ResolvableConjunction> dependencies(Concludable concludable) {
        return iterate(logicMgr.applicableRules(concludable).keySet()).map(rule -> rule.condition().conjunction()).toSet();
    }

    public ConjunctionSummary conjunctionSummary(ResolvableConjunction conjunction) {
        if (!conjunctionSummaries.containsKey(conjunction)) {
            recursivelySummarizeConjunctions(conjunction);
        }
        return conjunctionSummaries.get(conjunction);
    }

    private ConjunctionSummary createConjunctionSummary(ResolvableConjunction conjunction, Set<Pair<Concludable, ResolvableConjunction>> cyclicConcludables) {
        Set<Resolvable<?>> resolvables = logicMgr.compile(conjunction);
        Map<Concludable, Set<ResolvableConjunction>> cyclicDependencies = new HashMap<>();
        Map<Concludable, Set<ResolvableConjunction>> acyclicDependencies = new HashMap<>();
        iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
            cyclicDependencies.put(concludable, new HashSet<>());
            acyclicDependencies.put(concludable, new HashSet<>());
        });

        iterate(cyclicConcludables).forEachRemaining(concludableDependency -> {
            cyclicDependencies.get(concludableDependency.first()).add(concludableDependency.second());
        });

        iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable).forEachRemaining(concludable -> {
            iterate(dependencies(concludable))
                    .filter(dependency -> !cyclicDependencies.get(concludable).contains(dependency))
                    .forEachRemaining(dependency -> acyclicDependencies.get(concludable).add(dependency));
        });

        return new ConjunctionSummary(conjunction, resolvables, cyclicDependencies, acyclicDependencies);
    }

    private void recursivelySummarizeConjunctions(ResolvableConjunction root) {
        ConjunctionConcludableStack stack = new ConjunctionConcludableStack();
        Map<ResolvableConjunction, Set<Pair<Concludable, ResolvableConjunction>>> cyclicConcludables = new HashMap<>();
        findCyclicDependencies(root, stack, cyclicConcludables);
        iterate(stack.visited()).forEachRemaining(conjunction -> {
            conjunctionSummaries.put(conjunction, createConjunctionSummary(conjunction, cyclicConcludables.getOrDefault(conjunction, new HashSet<>())));
        });
    }

    private void findCyclicDependencies(ResolvableConjunction conjunction, ConjunctionConcludableStack stack, Map<ResolvableConjunction, Set<Pair<Concludable, ResolvableConjunction>>> cyclicConcludables) {
        if (conjunctionSummaries.containsKey(conjunction)) {
            return; // We only add conjunction summaries at the end of a search -> this does not loop back.
        }

        if (stack.contains(conjunction)) {
            ResolvableConjunction cyclicDependency = conjunction;
            List<ResolvableConjunction> stackPeek = stack.peekUntil(conjunction);
            for (ResolvableConjunction conj : stackPeek) {
                cyclicConcludables.computeIfAbsent(conj, c -> new HashSet<>()).add(new Pair<>(stack.getConcludable(conj), cyclicDependency));
                cyclicDependency = conj;
            }
        } else {
            stack.add(conjunction);
            Set<Resolvable<?>> resolvables = logicMgr.compile(conjunction);
            iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated)
                    .flatMap(negated -> iterate(negated.disjunction().conjunctions()))
                    .forEachRemaining(dependency -> conjunctionSummary(dependency)); // Stratified negation -> Fresh set
            iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .forEachRemaining(concludable -> {
                        stack.setConcludable(conjunction, concludable);
                        iterate(dependencies(concludable)).forEachRemaining(dependency -> findCyclicDependencies(dependency, stack, cyclicConcludables));
                    });
            assert stack.last() == conjunction;
            stack.pop();
        }
    }

    static class ConjunctionSummary {
        private final ResolvableConjunction conjunction;
        private final Set<Resolvable<?>> resolvables;
        private final Set<Variable> estimateableVars;
        private final Set<Concludable> cyclicConcludables;
        private final Set<Concludable> acyclicConcludables;
        private final Map<Concludable, Set<ResolvableConjunction>> cyclicDependencies;
        private final Map<Concludable, Set<ResolvableConjunction>> acyclicDependencies;

        ConjunctionSummary(ResolvableConjunction conjunction, Set<Resolvable<?>> resolvables,
                           Map<Concludable, Set<ResolvableConjunction>> cyclicDependencies, Map<Concludable, Set<ResolvableConjunction>> acyclicDependencies) {
            this.conjunction = conjunction;
            this.resolvables = resolvables;
            this.estimateableVars = ReasonerPlanner.estimateableVariables(conjunction.pattern().variables());
            this.cyclicDependencies = cyclicDependencies;
            this.acyclicDependencies = acyclicDependencies;
            this.cyclicConcludables = iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .filter(concludable -> !cyclicDependencies.get(concludable).isEmpty()).toSet();
            this.acyclicConcludables = iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable)
                    .filter(concludable -> cyclicDependencies.get(concludable).isEmpty()).toSet();
            assert cyclicDependencies.keySet().containsAll(iterate(resolvables).filter(Resolvable::isConcludable).toList());
            assert acyclicDependencies.keySet().containsAll(iterate(resolvables).filter(Resolvable::isConcludable).toList());
        }

        public Set<Resolvable<?>> resolvables() {
            return resolvables;
        }

        public Set<Retrievable> retrievables() {
            return iterate(resolvables).filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).toSet();
        }

        public Set<Negated> negateds() {
            return iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated).toSet();
        }

        public Set<Concludable> cyclicConcludables() {
            return cyclicConcludables;
        }

        public Set<Concludable> acyclicConcludables() {
            return acyclicConcludables;
        }

        public Set<ResolvableConjunction> cyclicDependencies(Concludable concludable) {
            return cyclicDependencies.get(concludable);
        }

        public Set<ResolvableConjunction> acyclicDependencies(Concludable concludable) {
            return acyclicDependencies.get(concludable);
        }


        public ResolvableConjunction conjunction() {
            return conjunction;
        }

        public Set<Variable> estimateableVars() {
            return estimateableVars;
        }
    }

    private static class ConjunctionConcludableStack {

        private final Set<ResolvableConjunction> visited;
        ArrayList<ResolvableConjunction> stack;
        Map<ResolvableConjunction, Concludable> concludables;

        ConjunctionConcludableStack() {
            this.stack = new ArrayList<>();
            this.visited = new HashSet<>();
            this.concludables = new HashMap<>();
        }

        private void add(ResolvableConjunction conjunction) {
            stack.add(conjunction);
            this.visited.add(conjunction);
            concludables.put(conjunction, null);
        }

        private ResolvableConjunction last() {
            return stack.get(stack.size() - 1);
        }

        private boolean contains(ResolvableConjunction conjunction) {
            return concludables.containsKey(conjunction);
        }

        private void pop() {
            ResolvableConjunction conjunction = stack.remove(stack.size() - 1);
            concludables.remove(conjunction);
        }

        private void setConcludable(ResolvableConjunction conjunction, Concludable concludable) {
            assert stack.contains(conjunction);
            concludables.put(conjunction, concludable);
        }

        private Concludable getConcludable(ResolvableConjunction conjunction) {
            return concludables.get(conjunction);
        }

        private Set<ResolvableConjunction> visited() {
            return visited;
        }

        private List<ResolvableConjunction> peekUntil(ResolvableConjunction untilInclusive) {
            assert stack.contains(untilInclusive);
            List<ResolvableConjunction> subList = new ArrayList<>();
            for (int i = stack.size() - 1; stack.get(i) != untilInclusive; i--) {
                subList.add(stack.get(i));
            }
            subList.add(untilInclusive);
            return subList;
        }
    }
}
