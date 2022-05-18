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
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
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
import static java.util.stream.Collectors.toMap;

public class Registry {

    private final static Logger LOG = LoggerFactory.getLogger(Registry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Concludable, Actor.Driver<ConcludableController.Match>> concludableControllers;
    private final Map<Actor.Driver<ConcludableController.Match>, Set<Concludable>> controllerConcludables;
    private final Map<Rule, Actor.Driver<ConditionController>> ruleConditions;
    private final Map<Rule, Actor.Driver<ConclusionController.Match>> ruleConclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final Map<Rule, Actor.Driver<ConclusionController.Explain>> explainRuleConclusions;
    private final Set<Actor.Driver<? extends AbstractController<?, ?, ?, ?, ?, ?>>> controllers;
    private final TraversalEngine traversalEngine;
    private final AbstractController.Context controllerContext;
    private final Actor.Driver<MaterialisationController> materialisationController;
    private final AtomicBoolean terminated;
    private TypeDBException terminationCause;

    public Registry(ActorExecutorGroup executorService, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                    LogicManager logicMgr, com.vaticle.typedb.core.common.parameters.Context.Query context) {
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableControllers = new ConcurrentHashMap<>();
        this.controllerConcludables = new ConcurrentHashMap<>();
        this.ruleConditions = new ConcurrentHashMap<>();
        this.ruleConclusions = new ConcurrentHashMap<>();
        this.explainRuleConclusions = new ConcurrentHashMap<>();
        this.controllers = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
        Tracer tracer = null;
        if (context.options().traceInference()) {
            tracer = new Tracer(context.transactionId(), context.options().reasonerDebuggerDir());
        }
        Tracer finalTracer = tracer;
        this.controllerContext = new AbstractController.Context(
                executorService, this, Actor.driver(driver -> new Monitor(driver, finalTracer), executorService), tracer
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

    public void terminate(Throwable e) {
        if (terminated.compareAndSet(false, true)) {
            terminationCause = TypeDBException.of(REASONING_TERMINATED_WITH_CAUSE, e);
            controllers.forEach(actor -> actor.execute(r -> r.terminate(terminationCause)));
            materialisationController.execute(actor -> actor.terminate(terminationCause));
            controllerContext.reactiveBlock().monitor().execute(actor -> actor.terminate(terminationCause));
        }
    }

    public void registerRootConjunction(Conjunction conjunction, Set<Variable.Retrievable> filter,
                                        boolean explain, ReasonerConsumer<ConceptMap> reasonerConsumer) {
        if (terminated.get()) {  // guard races without synchronized
            reasonerConsumer.exception(terminationCause);
            throw terminationCause;
        }
        LOG.debug("Creating Root Conjunction for: '{}'", conjunction);
        Actor.Driver<RootConjunctionController> controller =
                Actor.driver(driver -> new RootConjunctionController(
                        driver, conjunction, filter, explain, controllerContext, reasonerConsumer
                ), controllerContext.executorService());
        controllers.add(controller);
        controller.execute(RootConjunctionController::initialise);
    }

    public void registerRootDisjunction(Disjunction disjunction, Set<Variable.Retrievable> filter,
                                        boolean explain, ReasonerConsumer<ConceptMap> reasonerConsumer) {
        if (terminated.get()) {  // guard races without synchronized
            reasonerConsumer.exception(terminationCause);
            throw terminationCause;
        }
        LOG.debug("Creating Root Disjunction for: '{}'", disjunction);
        Actor.Driver<RootDisjunctionController> controller =
                Actor.driver(driver -> new RootDisjunctionController(
                                     driver, disjunction, filter, explain, controllerContext, reasonerConsumer
                             ), controllerContext.executorService()
                );
        controller.execute(RootDisjunctionController::initialise);
        controllers.add(controller);
    }

    public void registerExplainableRoot(Concludable concludable, ConceptMap bounds,
                                        ReasonerConsumer<Explanation> reasonerConsumer) {
        if (terminated.get()) {  // guard races without synchronized
            reasonerConsumer.exception(terminationCause);
            throw terminationCause;
        }
        Actor.Driver<ConcludableController.Explain> controller = Actor.driver(
                driver -> new ConcludableController.Explain(
                        driver, concludable, bounds, controllerContext, reasonerConsumer
                ), controllerContext.executorService()
        );
        controller.execute(ConcludableController.Explain::initialise);
        controllers.add(controller);
    }

    public Actor.Driver<NestedConjunctionController> registerNestedConjunction(Conjunction conjunction) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        LOG.debug("Creating Nested Conjunction for: '{}'", conjunction);
        Actor.Driver<NestedConjunctionController> controller =
                Actor.driver(driver -> new NestedConjunctionController(driver, conjunction, controllerContext),
                             controllerContext.executorService());
        controller.execute(ConjunctionController::initialise);
        controllers.add(controller);
        return controller;
    }

    public Actor.Driver<NestedDisjunctionController> registerNestedDisjunction(Disjunction disjunction) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        LOG.debug("Creating Nested Disjunction for: '{}'", disjunction);
        Actor.Driver<NestedDisjunctionController> controller =
                Actor.driver(driver -> new NestedDisjunctionController(driver, disjunction, controllerContext),
                             controllerContext.executorService());
        controller.execute(NestedDisjunctionController::initialise);
        controllers.add(controller);
        return controller;
    }

    public ResolverView.MappedConcludable registerOrGetConcludable(Concludable concludable) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        Optional<ResolverView.MappedConcludable> resolverViewOpt = getConcludable(concludable);
        ResolverView.MappedConcludable controllerView;
        if (resolverViewOpt.isPresent()) {
            LOG.debug("Got ConcludableResolver: '{}'", concludable.pattern());
            controllerView = resolverViewOpt.get();
            controllerConcludables.get(controllerView.controller()).add(concludable);
        } else {
            LOG.debug("Register ConcludableResolver: '{}'", concludable.pattern());
            Actor.Driver<ConcludableController.Match> controller = Actor.driver(
                    driver -> new ConcludableController.Match(driver, concludable, controllerContext),
                    controllerContext.executorService()
            );
            controller.execute(ConcludableController::initialise);
            controllerView = ResolverView.concludable(controller, identity(concludable));
            controllers.add(controller);
            concludableControllers.put(concludable, controllerView.controller());
            Set<Concludable> concludables = new HashSet<>();
            concludables.add(concludable);
            controllerConcludables.put(controllerView.controller(), concludables);
        }
        return controllerView;
    }

    private static Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream().collect(toMap(Function.identity(), Function.identity()));
    }

    private Optional<ResolverView.MappedConcludable> getConcludable(Concludable concludable) {
        for (Map.Entry<Concludable, Actor.Driver<ConcludableController.Match>> c : concludableControllers.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash - defer this in case alpha
            //  equivalence is no longer used.
            Optional<AlphaEquivalence> alphaEquality = concludable.alphaEquals(c.getKey()).first();
            if (alphaEquality.isPresent()) {
                return Optional.of(ResolverView.concludable(c.getValue(), alphaEquality.get().retrievableMapping()));
            }
        }
        return Optional.empty();
    }

    public ResolverView.FilteredRetrievable registerRetrievable(Retrievable retrievable) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        LOG.debug("Register RetrievableController: '{}'", retrievable.pattern());
        Actor.Driver<RetrievableController> controller = Actor.driver(
                driver -> new RetrievableController(driver, retrievable, controllerContext), controllerContext.executorService());
        controllers.add(controller);
        return ResolverView.retrievable(controller, retrievable.retrieves());
    }

    public ResolverView.FilteredNegation registerNegation(Negated negated, Conjunction conjunction) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        LOG.debug("Creating NegationController for : {}", negated);
        Actor.Driver<NegationController> negationController = Actor.driver(
                driver -> new NegationController(driver, negated, controllerContext), controllerContext.executorService());
        negationController.execute(NegationController::initialise);
        controllers.add(negationController);
        Set<Variable.Retrievable> filter = filter(conjunction, negated);
        return ResolverView.negation(negationController, filter);
    }

    private static Set<Variable.Retrievable> filter(Conjunction scope, Negated inner) {
        return scope.variables().stream()
                .filter(var -> var.id().isRetrievable() && inner.retrieves().contains(var.id().asRetrievable()))
                .map(var -> var.id().asRetrievable())
                .collect(Collectors.toSet());
    }

    public Actor.Driver<ConclusionController.Match> registerOrGetMatchConclusion(Rule.Conclusion conclusion) {
        LOG.debug("Register ConclusionController: '{}'", conclusion);
        Actor.Driver<ConclusionController.Match> controller = ruleConclusions.computeIfAbsent(conclusion.rule(), r -> {
            Actor.Driver<ConclusionController.Match> c = Actor.driver(
                    driver -> new ConclusionController.Match(
                            driver, conclusion, materialisationController, controllerContext
                    ), controllerContext.executorService()
            );
            c.execute(ConclusionController::initialise);
            return c;
        });
        controllers.add(controller);
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        return controller;
    }

    public Actor.Driver<ConclusionController.Explain> registerExplainConclusion(Rule.Conclusion conclusion) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        Actor.Driver<ConclusionController.Explain> controller = explainRuleConclusions.computeIfAbsent(
                conclusion.rule(), r -> {
                    LOG.debug("Register Explain ConclusionController: '{}'", conclusion);
                    Actor.Driver<ConclusionController.Explain> c = Actor.driver(
                            driver -> new ConclusionController.Explain(
                                    driver, conclusion, materialisationController, controllerContext
                            ), controllerContext.executorService());
                    c.execute(ConclusionController::initialise);
                    return c;
                }
        );
        controllers.add(controller);
        return controller;
    }

    public Actor.Driver<ConditionController> registerCondition(Rule.Condition condition) {
        if (terminated.get()) throw terminationCause; // guard races without synchronized
        Actor.Driver<ConditionController> controller = ruleConditions.computeIfAbsent(condition.rule(), r -> {
            LOG.debug("Register ConditionController: '{}'", condition);
            Actor.Driver<ConditionController> c = Actor.driver(
                    driver -> new ConditionController(driver, condition, controllerContext),
                    controllerContext.executorService()
            );
            c.execute(ConditionController::initialise);
            return c;
        });
        controllers.add(controller);
        return controller;
    }

    public void setExecutorService(ActorExecutorGroup executorService) {
        this.controllerContext.setExecutorService(executorService);
    }

    public void close() {
        controllerContext.tracer().ifPresent(Tracer::finishTrace);
    }

    public static abstract class ResolverView {

        public static MappedConcludable concludable(Actor.Driver<ConcludableController.Match> controller, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new MappedConcludable(controller, mapping);
        }

        public static FilteredNegation negation(Actor.Driver<NegationController> controller, Set<Variable.Retrievable> filter) {
            return new FilteredNegation(controller, filter);
        }

        public static FilteredRetrievable retrievable(Actor.Driver<RetrievableController> controller, Set<Variable.Retrievable> filter) {
            return new FilteredRetrievable(controller, filter);
        }

        public abstract Actor.Driver<? extends AbstractController<?, ?, ?, ?, ?, ?>> controller();

        public static class MappedConcludable extends ResolverView {
            private final Actor.Driver<ConcludableController.Match> controller;
            private final Map<Variable.Retrievable, Variable.Retrievable> mapping;

            public MappedConcludable(Actor.Driver<ConcludableController.Match> controller, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
                this.controller = controller;
                this.mapping = mapping;
            }

            public Map<Variable.Retrievable, Variable.Retrievable> mapping() {
                return mapping;
            }

            @Override
            public Actor.Driver<ConcludableController.Match> controller() {
                return controller;
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
