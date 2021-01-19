/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution;

import grakn.common.collection.Pair;
import grakn.core.common.concurrent.actor.Actor;
import grakn.core.common.concurrent.actor.EventLoopGroup;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.ResolvableResolver;
import grakn.core.reasoner.resolution.resolver.RetrievableResolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.reasoner.resolution.resolver.RuleResolver;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final ConceptManager conceptMgr;
    private final HashMap<Concludable, Actor<ConcludableResolver>> concludableActors;
    private final LogicManager logicMgr;
    private boolean explanations;
    private final HashMap<Rule, Actor<RuleResolver>> rules;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private EventLoopGroup elg;

    public ResolverRegistry(EventLoopGroup elg, Actor<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine,
                            ConceptManager conceptMgr, LogicManager logicMgr) {
        this.elg = elg;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.explanations = false; // TODO enable/disable explanations from transaction context
        concludableActors = new HashMap<>();
        rules = new HashMap<>();
    }

    public static List<Resolvable> plan(Set<Resolvable> resolvables, ConceptManager conceptMgr, LogicManager logicMgr) {
        return new Plan(resolvables, conceptMgr, logicMgr).plan();
    }

    public Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> registerResolvable(Resolvable resolvable) {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    public Actor<RuleResolver> registerRule(Rule rule) {
        LOG.debug("Register retrieval for rule actor: '{}'", rule);
        return rules.computeIfAbsent(rule, (r) -> Actor.create(elg, self -> new RuleResolver(self, r, this, traversalEngine, conceptMgr, logicMgr, explanations)));
    }

    public Actor<RootResolver> createRoot(final Conjunction pattern, final Consumer<ResolutionAnswer> onAnswer, Consumer<Integer> onExhausted) {
        LOG.debug("Creating Conjunction Actor for pattern: '{}'", pattern);
        return Actor.create(elg, self -> new RootResolver(self, pattern, onAnswer, onExhausted, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, explanations));
    }

    // for testing
    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.elg = eventLoopGroup;
    }

    private Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> registerRetrievable(Retrievable retrievable) {
        LOG.debug("Register retrieval for retrievable actor: '{}'", retrievable.conjunction());
        Actor<RetrievableResolver> retrievableActor = Actor.create(elg, self -> new RetrievableResolver(self, retrievable, this, traversalEngine, explanations));
        return new Pair<>(retrievableActor, identity(retrievable));
    }

    private Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> registerConcludable(Concludable concludable) {
        LOG.debug("Register retrieval for concludable actor: '{}'", concludable.conjunction());
        for (Map.Entry<Concludable, Actor<ConcludableResolver>> c : concludableActors.entrySet()) {
            // TODO This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = c.getKey().alphaEquals(concludable);
            if (alphaEquality.isValid()) {
                return new Pair<>(c.getValue(), alphaEquality.asValid().namedVariableMapping());
            }
        }
        Actor<ConcludableResolver> concludableActor = Actor.create(elg, self ->
                new ConcludableResolver(self, concludable, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, explanations));
        concludableActors.put(concludable, concludableActor);
        return new Pair<>(concludableActor, identity(concludable));
    }

    private static Map<Reference.Name, Reference.Name> identity(Resolvable resolvable) {
        return new HashSet<>(resolvable.conjunction().variables()).stream()
                .filter(variable -> variable.reference().isName())
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    private static class Plan {
        private final List<Resolvable> plan;
        private final Map<Resolvable, Set<Variable>> dependencies;
        private final ConceptManager conceptMgr;
        private final LogicManager logicMgr;
        private final Set<Variable> varsAnswered;
        private final Set<Resolvable> remaining;

        Plan(Set<Resolvable> resolvables, ConceptManager conceptMgr, LogicManager logicMgr) {
            this.conceptMgr = conceptMgr;
            this.logicMgr = logicMgr;
            assert resolvables.size() > 0;
            this.plan = new ArrayList<>();
            this.varsAnswered = new HashSet<>();
            this.dependencies = dependencies(resolvables);
            this.remaining = new HashSet<>(resolvables);

            planning();

            assert plan.size() == resolvables.size();
            assert set(plan).equals(resolvables);
        }

        private void add(Resolvable resolvable) {
            plan.add(resolvable);
            varsAnswered.addAll(resolvable.conjunction().variables());
            remaining.remove(resolvable);
        }

        private void planning() {
            while (remaining.size() != 0) {
                Optional<Concludable> concludable;
                Optional<Resolvable> retrievable;

                // Retrievable where:
                // all of it's dependencies are already satisfied,
                // which will answer the most variables
                retrievable = mostUnansweredVars(dependenciesSatisfied(connected(remaining.stream().filter(Resolvable::isRetrievable))));
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // all of it's dependencies are already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = fewestRules(dependenciesSatisfied(connected(remaining.stream().filter(Resolvable::isConcludable))));
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Retrievable where:
                // all of it's dependencies are already satisfied (should be moot),
                // it can be disconnected
                // which will answer the most variables
                retrievable = mostUnansweredVars(dependenciesSatisfied(remaining.stream().filter(Resolvable::isRetrievable)));
                if (retrievable.isPresent()) {
                    add(retrievable.get());
                    continue;
                }

                // Concludable where:
                // it can be disconnected
                // all of it's dependencies are already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = fewestRules(dependenciesSatisfied(remaining.stream().filter(Resolvable::isConcludable)));
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }

                // Concludable where:
                // it can be disconnected
                // all of it's dependencies are NOT already satisfied,
                // which has the least applicable rules,
                // and of those the least unsatisfied variables
                concludable = fewestRules(remaining.stream().filter(Resolvable::isConcludable));
                if (concludable.isPresent()) {
                    add(concludable.get());
                    continue;
                }
                assert false;
            }
        }

        private Stream<Resolvable> dependenciesSatisfied(Stream<Resolvable> resolvableStream) {
            return resolvableStream.filter(c -> varsAnswered.containsAll(dependencies.get(c)));
        }

        private Stream<Resolvable> connected(Stream<Resolvable> resolvableStream) {
            return resolvableStream.filter(r -> !Collections.disjoint(r.conjunction().variables(), varsAnswered));
        }

        private Optional<Concludable> fewestRules(Stream<Resolvable> resolvableStream) {
            // TODO How to do a tie-break for Concludables with the same number of applicable rules?
            return resolvableStream.map(Resolvable::asConcludable)
                    .min(Comparator.comparingInt(c -> c.getApplicableRules(conceptMgr, logicMgr).toSet().size()));
        }

        private Optional<Resolvable> mostUnansweredVars(Stream<Resolvable> resolvableStream) {
            return resolvableStream.max(Comparator.comparingInt(r -> {
                HashSet<Variable> s = new HashSet<>(r.conjunction().variables());
                s.removeAll(varsAnswered);
                return s.size();
            }));
        }

        public List<Resolvable> plan() {
            return plan;
        }

        /**
         * Determine the resolvables that are dependent upon the generation of each variable
         */
        private Map<Resolvable, Set<Variable>> dependencies(Set<Resolvable> resolvables) {
            Map<Resolvable, Set<Variable>> deps = new HashMap<>();
            Set<Variable> generatedVars = iterate(resolvables).filter(Resolvable::isConcludable)
                    .map(Resolvable::asConcludable).map(Concludable::generating).toSet();
            for (Resolvable resolvable : resolvables) {
                for (Variable v : resolvable.conjunction().variables()) {
                    deps.putIfAbsent(resolvable, new HashSet<>());
                    if (generatedVars.contains(v) && !(resolvable.isConcludable() && resolvable.asConcludable().generating().equals(v))) {
                        // TODO Should this rule the Resolvable out if generates it's own dependency?
                        deps.get(resolvable).add(v);
                    }
                }
            }
            return deps;
        }
    }
}
