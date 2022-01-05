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

package com.vaticle.typedb.core.reasoner.resolution;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.controller.ConcludableController;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController;
import com.vaticle.typedb.core.reasoner.controller.ConditionController;
import com.vaticle.typedb.core.reasoner.controller.RetrievableController;
import com.vaticle.typedb.core.reasoner.controller.RootConjunctionController;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Explain;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Match;
import com.vaticle.typedb.core.reasoner.resolution.framework.Materialiser;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver.BoundConcludableContext;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConclusionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundRetrievableResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConclusionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConditionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConjunctionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.DisjunctionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.NegationResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.RootResolver;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.RESOLUTION_TERMINATED;
import static java.util.stream.Collectors.toMap;

public class ControllerRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ControllerRegistry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Concludable, Actor.Driver<ConcludableController>> concludableControllers;
    private final Map<Actor.Driver<ConcludableController>, Set<Concludable>> controllerConcludables;
    private final ConcurrentMap<Rule, Actor.Driver<ConditionResolver>> ruleConditions;
    private final ConcurrentMap<Rule, Actor.Driver<ConclusionResolver>> ruleConclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final ConcurrentMap<Actor.Driver<ConclusionResolver>, Rule> conclusionRule;
    private final Set<Actor.Driver<? extends Resolver<?>>> resolvers;
    private final Set<Actor.Driver<? extends Controller<?,?,?,?>>> controllers;
    private final TraversalEngine traversalEngine;
    private final boolean resolutionTracing;
    private final AtomicBoolean terminated;
    private final Actor.Driver<Materialiser> materialiser;
    private ActorExecutorGroup executorService;

    public ControllerRegistry(ActorExecutorGroup executorService, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                              LogicManager logicMgr, boolean resolutionTracing) {
        this.executorService = executorService;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableControllers = new HashMap<>();
        this.controllerConcludables = new HashMap<>();
        this.ruleConditions = new ConcurrentHashMap<>();
        this.ruleConclusions = new ConcurrentHashMap<>();
        this.conclusionRule = new ConcurrentHashMap<>();
        this.resolvers = new ConcurrentSet<>();
        this.controllers = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
        this.resolutionTracing = resolutionTracing;
        this.materialiser = Actor.driver(driver -> new Materialiser(driver, this), executorService);
    }

    public TraversalEngine traversalEngine() {
        return traversalEngine;
    }

    public ConceptManager conceptManager() {
        return conceptMgr;
    }

    public LogicManager logicManager() {
        return logicMgr;
    }

    public boolean resolutionTracing() {
        return resolutionTracing;
    }

    public void terminate(Throwable cause) {
        if (terminated.compareAndSet(false, true)) {
            resolvers.forEach(actor -> actor.execute(r -> r.terminate(cause)));
            materialiser.execute(r -> r.terminate(cause));
        }
    }

    public Actor.Driver<RootConjunctionController> createRoot(Conjunction conjunction, Subscriber<ConceptMap> reasonerEntryPoint) {
        LOG.debug("Creating Root Conjunction for: '{}'", conjunction);
        Actor.Driver<RootConjunctionController> controller =
                Actor.driver(driver -> new RootConjunctionController(driver, conjunction, executorService, this,
                                                                     reasonerEntryPoint), executorService);
        controller.execute(actor -> actor.computeProcessorIfAbsent(new ConceptMap()));
        // TODO: Consider exception handling
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return controller;
    }

    public ResolverView.MappedConcludable registerConcludableController(Concludable concludable) {
        LOG.debug("Register ConcludableResolver: '{}'", concludable.pattern());
        Optional<ResolverView.MappedConcludable> resolverViewOpt = getConcludableResolver(concludable);
        ResolverView.MappedConcludable controllerView;
        if (resolverViewOpt.isPresent()) {
            controllerView = resolverViewOpt.get();
            controllerConcludables.get(controllerView.controller()).add(concludable);
        } else {
            Actor.Driver<ConcludableController> controller = Actor.driver(driver -> new ConcludableController(driver, concludable, executorService, this), executorService);
            controllerView = ResolverView.concludable(controller, identity(concludable));
            controllers.add(controller);
            concludableControllers.put(concludable, controllerView.controller());
            Set<Concludable> concludables = new HashSet<>();
            concludables.add(concludable);
            controllerConcludables.put(controllerView.controller(), concludables);
        }
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return controllerView;
    }

    private Optional<ResolverView.MappedConcludable> getConcludableResolver(Concludable concludable) {
        for (Map.Entry<Concludable, Actor.Driver<ConcludableController>> c : concludableControllers.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash
            Optional<AlphaEquivalence> alphaEquality = concludable.alphaEquals(c.getKey()).first();
            if (alphaEquality.isPresent()) {
                return Optional.of(ResolverView.concludable(c.getValue(), alphaEquality.get().retrievableMapping()));
            }
        }
        return Optional.empty();
    }

    public ResolverView.FilteredRetrievable registerRetrievableController(Retrievable retrievable) {
        LOG.debug("Register RetrievableController: '{}'", retrievable.pattern());
        Actor.Driver<RetrievableController> controller = Actor.driver(
                driver -> new RetrievableController(driver, "", retrievable, executorService, this), executorService);
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.retrievable(controller, retrievable.retrieves());
    }

//    public Pair<Actor.Driver<NegationController>, Set<Variable.Retrievable>> registerNegationController(Negation negation) {
//        return null;
//    }

    public Actor.Driver<ConclusionController> registerConclusionController(Rule.Conclusion conclusion) {
        return null;  // TODO
    }

    public Actor.Driver<ConditionController> registerConditionController(Rule.Condition condition) {
        return null; // TODO
    }

    public Actor.Driver<RootResolver.Disjunction> createRoot(Disjunction disjunction,
                                                             BiConsumer<Request, Match.Finished> onAnswer,
                                                             Consumer<Request> onExhausted,
                                                             Consumer<Throwable> onException) {
        LOG.debug("Creating Root.Disjunction for: '{}'", disjunction);
        Actor.Driver<RootResolver.Disjunction> resolver = Actor.driver(driver -> new RootResolver.Disjunction(
                driver, disjunction, onAnswer, onExhausted, onException, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public ResolverView.FilteredNegation negated(Negated negated, Conjunction upstream) {
        LOG.debug("Creating Negation resolver for : {}", negated);
        Actor.Driver<NegationResolver> negatedResolver = Actor.driver(
                driver -> new NegationResolver(driver, negated, this), executorService);
        resolvers.add(negatedResolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        Set<Variable.Retrievable> filter = filter(upstream, negated);
        return ResolverView.negation(negatedResolver, filter);
    }

    private static Set<Variable.Retrievable> filter(Conjunction scope, Negated inner) {
        return scope.variables().stream()
                .filter(var -> var.id().isRetrievable() && inner.retrieves().contains(var.id().asRetrievable()))
                .map(var -> var.id().asRetrievable())
                .collect(Collectors.toSet());
    }

    public Actor.Driver<ConditionResolver> registerCondition(Rule.Condition ruleCondition) {
        LOG.debug("Register retrieval for rule condition actor: '{}'", ruleCondition);
        Actor.Driver<ConditionResolver> resolver = ruleConditions.computeIfAbsent(ruleCondition.rule(), (r) -> Actor.driver(
                driver -> new ConditionResolver(driver, ruleCondition, this), executorService));
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public Actor.Driver<ConclusionResolver> registerConclusion(Rule.Conclusion conclusion) {
        LOG.debug("Register retrieval for rule conclusion actor: '{}'", conclusion);
        Actor.Driver<ConclusionResolver> resolver = ruleConclusions.computeIfAbsent(conclusion.rule(), r -> Actor.driver(
                driver -> new ConclusionResolver(driver, conclusion, this), executorService));
        conclusionRule.put(resolver, conclusion.rule());
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<BoundConclusionResolver> registerBoundConclusion(Rule.Conclusion conclusion, ConceptMap bounds) {
        LOG.debug("Register BoundConclusionResolver, pattern: {} bounds: {}", conclusion.conjunction(), bounds);
        Actor.Driver<BoundConclusionResolver> resolver = Actor.driver(driver -> new BoundConclusionResolver(
                driver, conclusion, bounds, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public ResolverView registerResolvable(Resolvable<?> resolvable) {
        if (resolvable.isRetrievable()) {
            return null;  // TODO: Remove
        } else if (resolvable.isConcludable()) {
            return null;  // TODO: Remove
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public Actor.Driver<BoundRetrievableResolver> registerBoundRetrievable(Retrievable retrievable, ConceptMap bounds) {
        LOG.debug("Register BoundRetrievableResolver, pattern: {} bounds: {}", retrievable.pattern(), bounds);
        Actor.Driver<BoundRetrievableResolver> resolver = Actor.driver(
                driver -> new BoundRetrievableResolver(driver, retrievable, bounds, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<BoundConcludableResolver.Exploring> registerExploring(ConceptMap bounds,
                                                                              BoundConcludableContext context) {
        LOG.debug("Register Exploring BoundConcludableResolver, pattern: {} bounds: {}", context.concludable().pattern(), bounds);
        Actor.Driver<BoundConcludableResolver.Exploring> resolver = Actor.driver(
                driver -> new BoundConcludableResolver.Exploring(driver, context, bounds, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<BoundConcludableResolver.Blocked> registerBlocked(ConceptMap bounds,
                                                                          BoundConcludableContext context) {
        LOG.debug("Register Blocked BoundConcludableResolver, pattern: {} bounds: {}", context.concludable().pattern(), bounds);
        Actor.Driver<BoundConcludableResolver.Blocked> resolver = Actor.driver(
                driver -> new BoundConcludableResolver.Blocked(driver, context, bounds, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<ConjunctionResolver.Nested> nested(Conjunction conjunction) {
        LOG.debug("Creating Conjunction resolver for : {}", conjunction);
        Actor.Driver<ConjunctionResolver.Nested> resolver = Actor.driver(
                driver -> new ConjunctionResolver.Nested(driver, conjunction, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<DisjunctionResolver.Nested> nested(Disjunction disjunction) {
        LOG.debug("Creating Disjunction resolver for : {}", disjunction);
        Actor.Driver<DisjunctionResolver.Nested> resolver = Actor.driver(
                driver -> new DisjunctionResolver.Nested(driver, disjunction, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    private static Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream().collect(toMap(Function.identity(), Function.identity()));
    }

    public Actor.Driver<RootResolver.Explain> explainer(Conjunction conjunction,
                                                        BiConsumer<Request, Explain.Finished> requestAnswered,
                                                        Consumer<Request> requestFailed, Consumer<Throwable> exception) {
        Actor.Driver<RootResolver.Explain> resolver = Actor.driver(
                driver -> new RootResolver.Explain(
                        driver, conjunction, requestAnswered, requestFailed, exception, this), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public void setExecutorService(ActorExecutorGroup executorService) {
        this.executorService = executorService;
    }

    public Actor.Driver<Materialiser> materialiser() {
        return materialiser;
    }

    public Actor.Driver<ConditionResolver> conditionResolver(Rule rule) {
        return ruleConditions.get(rule);
    }

    public Set<Concludable> concludables(Actor.Driver<ConcludableController> controller) {
        return controllerConcludables.get(controller);
    }

    public Rule conclusionRule(Actor.Driver<ConclusionResolver> resolver) {
        return conclusionRule.get(resolver);
    }

    public static abstract class ResolverView {

        public static MappedConcludable concludable(Actor.Driver<ConcludableController> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new MappedConcludable(resolver, mapping);
        }

        public static FilteredNegation negation(Actor.Driver<NegationResolver> resolver, Set<Variable.Retrievable> filter) {
            return new FilteredNegation(resolver, filter);
        }

        public static FilteredRetrievable retrievable(Actor.Driver<RetrievableController> controller, Set<Variable.Retrievable> filter) {
            return new FilteredRetrievable(controller, filter);
        }

        public boolean isMappedConcludable() { return false; }

        public boolean isFilteredNegation() { return false; }

        public boolean isFilteredRetrievable() { return false; }

        public MappedConcludable asMappedConcludable() {
            throw TypeDBException.of(ILLEGAL_CAST, getClass(), MappedConcludable.class);
        }

        public FilteredNegation asFilteredNegation() {
            throw TypeDBException.of(ILLEGAL_CAST, getClass(), FilteredNegation.class);
        }

        public FilteredRetrievable asFilteredRetrievable() {
            throw TypeDBException.of(ILLEGAL_CAST, getClass(), FilteredRetrievable.class);
        }

        public abstract Actor.Driver<? extends Controller<?, ?, ?, ?>> controller();

        public static class MappedConcludable extends ResolverView {
            private final Actor.Driver<ConcludableController> resolver;
            private final Map<Variable.Retrievable, Variable.Retrievable> mapping;

            public MappedConcludable(Actor.Driver<ConcludableController> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
                this.resolver = resolver;
                this.mapping = mapping;
            }

            public Map<Variable.Retrievable, Variable.Retrievable> mapping() {
                return mapping;
            }

            @Override
            public boolean isMappedConcludable() {
                return true;
            }

            @Override
            public MappedConcludable asMappedConcludable() {
                return this;
            }

            @Override
            public Actor.Driver<ConcludableController> controller() {
                return resolver;
            }
        }

        public static class FilteredNegation extends ResolverView {
            private final Actor.Driver<NegationResolver> resolver;
            private final Set<Variable.Retrievable> filter;

            public FilteredNegation(Actor.Driver<NegationResolver> resolver, Set<Variable.Retrievable> filter) {
                this.resolver = resolver;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public boolean isFilteredNegation() {
                return true;
            }

            @Override
            public FilteredNegation asFilteredNegation() {
                return this;
            }

            @Override
            public Actor.Driver<? extends Controller<?, ?, ?, ?>> controller() {
                return null;  // TODO
            }
        }

        public static class FilteredRetrievable extends ResolverView {
            private final Actor.Driver<RetrievableController> controller;
            private final Set<Variable.Retrievable> filter;

            public FilteredRetrievable(Actor.Driver<RetrievableController> controller, Set<Variable.Retrievable> filter) {
                this.controller = controller;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public boolean isFilteredRetrievable() { return true; }

            @Override
            public FilteredRetrievable asFilteredRetrievable() {
                return this;
            }

            @Override
            public Actor.Driver<RetrievableController> controller() {
                return controller;
            }
        }
    }
}
