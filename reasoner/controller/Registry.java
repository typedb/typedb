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

package com.vaticle.typedb.core.reasoner.controller;

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
import com.vaticle.typedb.core.reasoner.ReasonerProducer.EntryPoint;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.RESOLUTION_TERMINATED_WITH_CAUSE;
import static java.util.stream.Collectors.toMap;

public class Registry {

    private final static Logger LOG = LoggerFactory.getLogger(Registry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Concludable, Actor.Driver<ConcludableController>> concludableControllers;
    private final Map<Actor.Driver<ConcludableController>, Set<Concludable>> controllerConcludables;
    private final ConcurrentMap<Rule, Actor.Driver<ConditionController>> ruleConditions;
    private final ConcurrentMap<Rule, Actor.Driver<ConclusionController>> ruleConclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final Set<Actor.Driver<? extends Controller<?, ?, ?, ?, ?>>> controllers;
    private final TraversalEngine traversalEngine;
    private final boolean resolutionTracing;
    private final Actor.Driver<MaterialiserController> materialiserController;
    private final AtomicBoolean terminated;
    private Throwable terminationCause;
    private ActorExecutorGroup executorService;

    public Registry(ActorExecutorGroup executorService, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                    LogicManager logicMgr, boolean resolutionTracing) {
        this.executorService = executorService;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableControllers = new HashMap<>();
        this.controllerConcludables = new HashMap<>();
        this.ruleConditions = new ConcurrentHashMap<>();
        this.ruleConclusions = new ConcurrentHashMap<>();
        this.controllers = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
        this.resolutionTracing = resolutionTracing;
        this.materialiserController = Actor.driver(driver -> new MaterialiserController(
                driver, executorService, this, traversalEngine(), conceptManager()), executorService
        );
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
            terminationCause = cause;
            controllers.forEach(actor -> actor.execute(r -> r.terminate(cause)));
            materialiserController.execute(actor -> actor.terminate(cause));
        }
    }

    public Actor.Driver<RootConjunctionController> createRootConjunctionController(Conjunction conjunction,
                                                                                   Set<Variable.Retrievable> filter,
                                                                                   EntryPoint reasonerEntryPoint) {
        LOG.debug("Creating Root Conjunction for: '{}'", conjunction);
        Actor.Driver<RootConjunctionController> controller =
                Actor.driver(driver -> new RootConjunctionController(driver, conjunction, filter, executorService, this,
                                                                     reasonerEntryPoint), executorService);
        controller.execute(RootConjunctionController::setUpUpstreamProviders);
        controller.execute(actor -> actor.computeProcessorIfAbsent(new ConceptMap()));
        // TODO: Consider exception handling
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return controller;
    }

    public Actor.Driver<RootDisjunctionController> createRootDisjunctionController(Disjunction disjunction,
                                                                                   Set<Variable.Retrievable> filter,
                                                                                   EntryPoint reasonerEntryPoint) {
        LOG.debug("Creating Root Disjunction for: '{}'", disjunction);
        Actor.Driver<RootDisjunctionController> controller =
                Actor.driver(driver -> new RootDisjunctionController(driver, disjunction, filter, executorService, this,
                                                                     reasonerEntryPoint), executorService);
        controller.execute(RootDisjunctionController::setUpUpstreamProviders);
        controller.execute(actor -> actor.computeProcessorIfAbsent(new ConceptMap()));
        // TODO: Consider exception handling
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return controller;
    }

    public Actor.Driver<NestedConjunctionController> registerNestedConjunctionController(Conjunction conjunction) {
        LOG.debug("Creating Nested Conjunction for: '{}'", conjunction);
        Actor.Driver<NestedConjunctionController> controller =
                Actor.driver(driver -> new NestedConjunctionController(driver, conjunction, executorService, this),
                             executorService);
        controller.execute(ConjunctionController::setUpUpstreamProviders);
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return controller;
    }

    public Actor.Driver<NestedDisjunctionController> registerNestedDisjunctionController(Disjunction disjunction) {
        LOG.debug("Creating Nested Disjunction for: '{}'", disjunction);
        Actor.Driver<NestedDisjunctionController> controller =
                Actor.driver(driver -> new NestedDisjunctionController(driver, disjunction, executorService, this),
                             executorService);
        controller.execute(NestedDisjunctionController::setUpUpstreamProviders);
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
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
            controller.execute(ConcludableController::setUpUpstreamProviders);
            controllerView = ResolverView.concludable(controller, identity(concludable));
            controllers.add(controller);
            concludableControllers.put(concludable, controllerView.controller());
            Set<Concludable> concludables = new HashSet<>();
            concludables.add(concludable);
            controllerConcludables.put(controllerView.controller(), concludables);
        }
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return controllerView;
    }

    private static Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream().collect(toMap(Function.identity(), Function.identity()));
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
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return ResolverView.retrievable(controller, retrievable.retrieves());
    }

    public ResolverView.FilteredNegation registerNegationController(Negated negated, Conjunction conjunction) {
        LOG.debug("Creating NegationController for : {}", negated);
        Actor.Driver<NegationController> negationController = Actor.driver(
                driver -> new NegationController(driver, negated, executorService, this), executorService);
        negationController.execute(NegationController::setUpUpstreamProviders);
        controllers.add(negationController);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        Set<Variable.Retrievable> filter = filter(conjunction, negated);
        return ResolverView.negation(negationController, filter);
    }

    private static Set<Variable.Retrievable> filter(Conjunction scope, Negated inner) {
        return scope.variables().stream()
                .filter(var -> var.id().isRetrievable() && inner.retrieves().contains(var.id().asRetrievable()))
                .map(var -> var.id().asRetrievable())
                .collect(Collectors.toSet());
    }

    public Actor.Driver<ConclusionController> registerConclusionController(Rule.Conclusion conclusion) {
        LOG.debug("Register ConclusionController: '{}'", conclusion);
        Actor.Driver<ConclusionController> controller = ruleConclusions.computeIfAbsent(conclusion.rule(), r -> {
            Actor.Driver<ConclusionController> c = Actor.driver(
                    driver -> new ConclusionController(driver, "", conclusion, executorService,
                                                       materialiserController, this), executorService);
            c.execute(ConclusionController::setUpUpstreamProviders);
            return c;
        });
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return controller;
    }

    public Actor.Driver<ConditionController> registerConditionController(Rule.Condition condition) {
        LOG.debug("Register ConditionController: '{}'", condition);
        Actor.Driver<ConditionController> controller = ruleConditions.computeIfAbsent(condition.rule(), r -> {
            Actor.Driver<ConditionController> c = Actor.driver(
                    driver -> new ConditionController(driver, condition, executorService, this), executorService);
            c.execute(ConditionController::setUpUpstreamProviders);
            return c;
        });
        controllers.add(controller);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
        return controller;
    }

//    public Actor.Driver<RootResolver.Explain> explainer(Conjunction conjunction,
//                                                        BiConsumer<Request, Explain.Finished> requestAnswered,
//                                                        Consumer<Request> requestFailed, Consumer<Throwable> exception) {
//        Actor.Driver<RootResolver.Explain> resolver = Actor.driver(
//                driver -> new RootResolver.Explain(
//                        driver, conjunction, requestAnswered, requestFailed, exception, this), executorService);
//        resolvers.add(resolver);
//        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED_WITH_CAUSE, terminationCause); // guard races without synchronized
//        return resolver;
//    }

    public void setExecutorService(ActorExecutorGroup executorService) {
        this.executorService = executorService;
    }

    public static abstract class ResolverView {

        public static MappedConcludable concludable(Actor.Driver<ConcludableController> controller, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new MappedConcludable(controller, mapping);
        }

        public static FilteredNegation negation(Actor.Driver<NegationController> controller, Set<Variable.Retrievable> filter) {
            return new FilteredNegation(controller, filter);
        }

        public static FilteredRetrievable retrievable(Actor.Driver<RetrievableController> controller, Set<Variable.Retrievable> filter) {
            return new FilteredRetrievable(controller, filter);
        }

        public abstract Actor.Driver<? extends Controller<?, ?, ?, ?, ?>> controller();

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
            public Actor.Driver<ConcludableController> controller() {
                return resolver;
            }
        }

        public static class FilteredNegation extends ResolverView {
            private final Actor.Driver<NegationController> controller;
            private final Set<Variable.Retrievable> filter;

            public FilteredNegation(Actor.Driver<NegationController> controller, Set<Variable.Retrievable> filter) {
                this.controller = controller;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public Actor.Driver<NegationController> controller() {
                return controller;
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
            public Actor.Driver<RetrievableController> controller() {
                return controller;
            }
        }
    }
}
