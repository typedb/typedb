package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.*;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class GreedyAnswerSizeSearch extends PlanSearch {

    public GreedyAnswerSizeSearch(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
        super(traversalEng, conceptMgr, logicMgr);
    }

    /* TODO:
     * AnswerSize -> For each resolvable: Add the number of calls + the answers produced
     * AnswersProduced? TraversalEngine tells us this based on constraints in the rule-body
     * #calls? Product of the cardinality of each variable in the bounds (This number reduces as you evaluate a new resolvable which further constrains the variable - but min or less?)ssssssss
     * Effect? Schedule stronger constraints first.
     */
    @Override
    protected Plan<Resolvable<?>> planConjunction(ResolvableConjunction conjunction, Set<Identifier.Variable.Retrievable> inputBounds) {
        Set<Identifier.Variable.Retrievable> bounds = new HashSet<>(inputBounds);
        Set<Resolvable<?>> resolvables = new HashSet<>();

        Pair<Set<Concludable>, Set<Retrievable>> compiled = compile(conjunction);
        resolvables.addAll(compiled.first());
        resolvables.addAll(compiled.second());
        resolvables.addAll(conjunction.negations());

        Set<Resolvable<?>> remaining = new HashSet<>(resolvables);
        long cost = 0;
        List<Resolvable<?>> orderedResolvables = new ArrayList<>();
        Map<Resolvable<?>, Set<Identifier.Variable.Retrievable>> dependencies = dependencies(resolvables);


        while (!remaining.isEmpty()) {
            Optional<Resolvable<?>> nextResolvableOpt = remaining.stream()
                    .filter(r -> dependenciesSatisfied(r, bounds, dependencies))
                    .min(Comparator.comparing(r -> estimateCost(r, bounds)));

            if (!nextResolvableOpt.isPresent()) {
                nextResolvableOpt = remaining.stream()
                        .min(Comparator.comparing(r -> estimateCost(r, bounds)));
            }
            assert nextResolvableOpt.isPresent();
            Resolvable<?> nextResolvable = nextResolvableOpt.get();
            cost += estimateCost(nextResolvable, bounds); // TODO: Eliminate double work
            orderedResolvables.add(nextResolvable);
            remaining.remove(nextResolvable);
            bounds.addAll(nextResolvable.retrieves());
        }
        assert resolvables.size() == orderedResolvables.size() && iterate(orderedResolvables).allMatch(r -> resolvables.contains(r));
        return new Plan(orderedResolvables, cost);
    }

    protected long estimateCost(Resolvable<?> resolvable, Set<Identifier.Variable.Retrievable> bounds) {
        long cost = 0;
        if (resolvable.isRetrievable()) {
            cost += estimateAnswerCount(resolvable.asRetrievable().pattern(), resolvable.asRetrievable().retrieves());
        }
        if (resolvable.isConcludable()) {
            cost += iterate(logicMgr.applicableRules(resolvable.asConcludable()).keySet())
                    .map(rule -> estimateAnswerCount(rule.condition().pattern(), iterate(rule.then().retrieves()).filter(v -> rule.when().retrieves().contains(v)).toSet()))
                    .reduce(0L, Long::sum);
        }
        if (resolvable.isNegated()) {
            cost += 1; // TODO ? But the answerEstimate will be 1 because the filter is empty.
        }
        return cost;
    }

    private long estimateAnswerCount(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter) {
        return traversalEng.estimateAnswers(conjunction.traversal(filter));
    }

    public static class OldPlannerEmulator extends GreedyAnswerSizeSearch{

        public OldPlannerEmulator(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr) {
            super(traversalEng, conceptMgr, logicMgr);
        }

        @Override
        protected long estimateCost(Resolvable<?> r, Set<Identifier.Variable.Retrievable> bounds) {
            long cost = 0;
            cost += r.retrieves().stream().anyMatch(v -> bounds.contains(v)) ? 0 : 10; // Connected:disconnected
            if (r.isRetrievable()) {
                cost += 1;
            } else if (r.isConcludable()) {
                cost += 2;
            } else if (r.isNegated()) { // always at the end
                cost += 1000;
            }
            return cost;
        }
    }
}