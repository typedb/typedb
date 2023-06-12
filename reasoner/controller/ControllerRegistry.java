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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.Actor.Driver;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.reasoner.processor.reactive.Monitor;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONING_TERMINATED_WITH_CAUSE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.stream.Collectors.toMap;

public class ControllerRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ControllerRegistry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Concludable, Driver<ConcludableController.Match>> concludableControllers;
    private final Map<Driver<ConcludableController.Match>, Set<Concludable>> controllerConcludables;
    private final Map<Rule, Driver<ConditionController>> conditions;
    private final Map<Rule, Driver<ConclusionController.Match>> conclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final Map<Rule, Driver<ConclusionController.Explain>> explainConclusions;
    private final Set<Driver<? extends AbstractController<?, ?, ?, ?, ?, ?>>> controllers;
    private final TraversalEngine traversalEngine;
    private final AbstractController.Context controllerContext;
    private final Driver<MaterialisationController> materialisationController;
    private final AtomicBoolean terminated;
    private TypeDBException terminationCause;

    public ControllerRegistry(ActorExecutorGroup executorService, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                              LogicManager logicMgr, ReasonerPlanner reasonerPlanner, ReasonerPerfCounters perfCounters, Context.Transaction context) {
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableControllers = new ConcurrentHashMap<>();
        this.controllerConcludables = new ConcurrentHashMap<>();
        this.conditions = new ConcurrentHashMap<>();
        this.conclusions = new ConcurrentHashMap<>();
        this.explainConclusions = new ConcurrentHashMap<>();
        this.controllers = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
        Tracer tracer = null;
        if (context.options().traceInference()) {
            tracer = new Tracer(context.transactionId(), context.options().reasonerDebuggerDir());
        }
        Tracer finalTracer = tracer;
        this.controllerContext = new AbstractController.Context(
                executorService, this, Actor.driver(driver -> new Monitor(driver, finalTracer), executorService),
                reasonerPlanner, perfCounters, tracer
        );
        this.materialisationController = Actor.driver(driver -> new MaterialisationController(
                driver, controllerContext, traversalEngine(), conceptManager()), executorService
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

    public void terminate(Throwable cause) {
        if (terminated.compareAndSet(false, true)) {
            terminationCause = TypeDBException.of(REASONING_TERMINATED_WITH_CAUSE, cause);
            controllers.forEach(actor -> actor.executeNext(a -> a.terminate(terminationCause)));
            materialisationController.executeNext(a -> a.terminate(terminationCause));
            controllerContext.processor().monitor().executeNext(a -> a.terminate(terminationCause));
        }
    }

    public ReasonerPerfCounters perfCounters() {
        return controllerContext.processor().perfCounters();
    }

    private <C extends AbstractController<?, ?, ?, ?, ?, C>> void createRootController(
            ReasonerConsumer<?> reasonerConsumer, Function<Driver<C>, C> actorFn
    ) {
        if (terminated.get()) {  // guard races without synchronized
            reasonerConsumer.exception(terminationCause);
            throw terminationCause;
        }
        Driver<C> controller = Actor.driver(actorFn, controllerContext.executorService());
        controllers.add(controller);
        controller.execute(c -> c.initialise());
    }

    private <C extends AbstractController<?, ?, ?, ?, ?, C>> Driver<C> createController(Function<Driver<C>, C> actorFn) {
        if (terminated.get()) {  // guard races without synchronized
            throw terminationCause;
        }
        Driver<C> controller = Actor.driver(actorFn, controllerContext.executorService());
        controllers.add(controller);
        controller.execute(c -> c.initialise());
        return controller;
    }

    public void createRootConjunction(ResolvableConjunction conjunction, Modifiers.Filter filter,
                                      boolean explain, ReasonerConsumer<ConceptMap> reasonerConsumer) {
        Function<Driver<RootConjunctionController>, RootConjunctionController> actorFn = driver ->
                new RootConjunctionController(driver, conjunction, filter, explain, controllerContext, reasonerConsumer);
        LOG.debug("Create Root Conjunction for: '{}'", conjunction);
        createRootController(reasonerConsumer, actorFn);
    }

    public void createRootDisjunction(ResolvableDisjunction disjunction, Modifiers.Filter filter,
                                      boolean explain, ReasonerConsumer<ConceptMap> reasonerConsumer) {
        Function<Driver<RootDisjunctionController>, RootDisjunctionController> actorFn =
                driver -> new RootDisjunctionController(driver, disjunction, filter, explain, controllerContext, reasonerConsumer);
        LOG.debug("Create Root Disjunction for: '{}'", disjunction);
        createRootController(reasonerConsumer, actorFn);
    }

    public void createExplainableRoot(Concludable concludable, ConceptMap bounds, ReasonerConsumer<Explanation> reasonerConsumer) {
        Function<Driver<ConcludableController.Explain>, ConcludableController.Explain> actorFn =
                driver -> new ConcludableController.Explain(driver, concludable, bounds, controllerContext, reasonerConsumer);
        LOG.debug("Create Explainable Root for: '{}'", concludable);
        createRootController(reasonerConsumer, actorFn);
    }

    Driver<NestedConjunctionController> createNestedConjunction(ResolvableConjunction conjunction, Set<Variable.Retrievable> outputVariables) {
        // TODO: get output variables as an argument
        Function<Driver<NestedConjunctionController>, NestedConjunctionController> actorFn =
                driver -> new NestedConjunctionController(driver, conjunction, outputVariables, controllerContext);
        LOG.debug("Create Nested Conjunction for: '{}'", conjunction);
        return createController(actorFn);
    }

    Driver<NestedDisjunctionController> createNestedDisjunction(ResolvableDisjunction disjunction, Set<Variable.Retrievable> outputVariables) {
        Function<Driver<NestedDisjunctionController>, NestedDisjunctionController> actorFn =
                driver -> new NestedDisjunctionController(driver, disjunction, outputVariables, controllerContext);
        LOG.debug("Create Nested Disjunction for: '{}'", disjunction);
        return createController(actorFn);
    }

    ControllerView.MappedConcludable getOrCreateConcludable(Concludable concludable) {
        Optional<ControllerView.MappedConcludable> controllerViewOpt = getConcludable(concludable);
        ControllerView.MappedConcludable controllerView;
        if (controllerViewOpt.isPresent()) {
            controllerView = controllerViewOpt.get();
            LOG.debug("Got ConcludableController: '{}'", concludable.pattern());
            controllerConcludables.get(controllerView.controller()).add(concludable);
        } else {
            Function<Driver<ConcludableController.Match>, ConcludableController.Match> actorFn =
                    driver -> new ConcludableController.Match(driver, concludable, controllerContext);
            LOG.debug("Create ConcludableController: '{}'", concludable.pattern());
            controllerView = ControllerView.concludable(createController(actorFn), identity(concludable));
            concludableControllers.put(concludable, controllerView.controller());
            Set<Concludable> concludables = new HashSet<>();
            concludables.add(concludable);
            controllerConcludables.put(controllerView.controller(), concludables);
            controllerView.controller().execute(ConcludableController::initialise);
        }
        return controllerView;
    }

    private static Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream().collect(toMap(Function.identity(), Function.identity()));
    }

    private Optional<ControllerView.MappedConcludable> getConcludable(Concludable concludable) {
        for (Map.Entry<Concludable, Driver<ConcludableController.Match>> c : concludableControllers.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash - defer this in case alpha
            //  equivalence is no longer used.
            Optional<AlphaEquivalence> alphaEquality = concludable.alphaEquals(c.getKey()).first();
            if (alphaEquality.isPresent()) {
                return Optional.of(ControllerView.concludable(c.getValue(), alphaEquality.get().retrievableMapping()));
            }
        }
        return Optional.empty();
    }

    ControllerView.FilteredRetrievable createRetrievable(Retrievable retrievable) {
        Function<Driver<RetrievableController>, RetrievableController> actorFn =
                driver -> new RetrievableController(driver, retrievable, controllerContext);
        LOG.debug("Create RetrievableController: '{}'", retrievable.pattern());
        return ControllerView.retrievable(createController(actorFn), Modifiers.Filter.create(retrievable.retrieves()));
    }

    ControllerView.FilteredNegation createNegation(Negated negated, ResolvableConjunction conjunction) {
        Modifiers.Filter commonVariables = filter(conjunction, negated);
        Function<Driver<NegationController>, NegationController> actorFn =
                driver -> new NegationController(driver, negated, commonVariables.variables(), controllerContext);
        LOG.debug("Create NegationController for : {}", negated);
        return ControllerView.negation(createController(actorFn), commonVariables);
    }

    private static Modifiers.Filter filter(ResolvableConjunction scope, Negated inner) {
        return Modifiers.Filter.create(scope.pattern().variables().stream()
                .filter(var -> var.id().isRetrievable() && inner.retrieves().contains(var.id().asRetrievable()))
                .map(var -> var.id().asRetrievable())
                .collect(Collectors.toSet()));
    }

    Driver<ConclusionController.Match> getOrCreateMatchConclusion(Rule.Conclusion conclusion) {
        return conclusions.computeIfAbsent(conclusion.rule(), r -> {
            Function<Driver<ConclusionController.Match>, ConclusionController.Match> actorFn =
                    driver -> new ConclusionController.Match(driver, conclusion, materialisationController, controllerContext);
            LOG.debug("Create ConclusionController: '{}'", conclusion);
            return createController(actorFn);
        });
    }

    Driver<ConclusionController.Explain> getOrCreateExplainConclusion(Rule.Conclusion conclusion) {
        return explainConclusions.computeIfAbsent(
                conclusion.rule(), r -> {
                    Function<Driver<ConclusionController.Explain>, ConclusionController.Explain> actorFn =
                            driver -> new ConclusionController.Explain(driver, conclusion, materialisationController, controllerContext);
                    LOG.debug("Create Explain ConclusionController: '{}'", conclusion);
                    return createController(actorFn);
                }
        );
    }

    Driver<ConditionController> getOrCreateCondition(Rule.Condition condition) {
        return conditions.computeIfAbsent(condition.rule(), r -> {
            Function<Driver<ConditionController>, ConditionController> actorFn =
                    driver -> new ConditionController(driver, condition, controllerContext);
            LOG.debug("Create ConditionController: '{}'", condition);
            return createController(actorFn);
        });
    }

    public void setExecutorService(ActorExecutorGroup executorService) {
        this.controllerContext.setExecutorService(executorService);
    }

    public void close() {
        controllerContext.tracer().ifPresent(Tracer::finishTrace);
        controllerContext.processor().perfCounters().logCounters();
    }

    public static abstract class ControllerView {

        private static MappedConcludable concludable(Driver<ConcludableController.Match> controller,
                                                     Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new MappedConcludable(controller, mapping);
        }

        private static FilteredNegation negation(Driver<NegationController> controller, Modifiers.Filter filter) {
            return new FilteredNegation(controller, filter);
        }

        private static FilteredRetrievable retrievable(Driver<RetrievableController> controller, Modifiers.Filter filter) {
            return new FilteredRetrievable(controller, filter);
        }

        public abstract Driver<? extends AbstractController<?, ?, ?, ?, ?, ?>> controller();

        public static class MappedConcludable extends ControllerView {
            private final Driver<ConcludableController.Match> controller;
            private final Map<Variable.Retrievable, Variable.Retrievable> mapping;

            private MappedConcludable(Driver<ConcludableController.Match> controller, Map<Variable.Retrievable,
                    Variable.Retrievable> mapping) {
                this.controller = controller;
                this.mapping = mapping;
            }

            public Map<Variable.Retrievable, Variable.Retrievable> mapping() {
                return mapping;
            }

            @Override
            public Driver<ConcludableController.Match> controller() {
                return controller;
            }
        }

        public static class FilteredNegation extends ControllerView {
            private final Driver<NegationController> controller;
            private final Modifiers.Filter filter;

            private FilteredNegation(Driver<NegationController> controller, Modifiers.Filter filter) {
                this.controller = controller;
                this.filter = filter;
            }

            public Modifiers.Filter filter() {
                return filter;
            }

            @Override
            public Driver<NegationController> controller() {
                return controller;
            }
        }

        public static class FilteredRetrievable extends ControllerView {
            private final Driver<RetrievableController> controller;
            private final Modifiers.Filter filter;

            private FilteredRetrievable(Driver<RetrievableController> controller, Modifiers.Filter filter) {
                this.controller = controller;
                this.filter = filter;
            }

            public Modifiers.Filter filter() {
                return filter;
            }

            @Override
            public Driver<RetrievableController> controller() {
                return controller;
            }
        }
    }
}
