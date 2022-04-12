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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule.Conclusion;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock.Connector.AbstractRequest;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Input;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.reactive.Source;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.utils.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ConcludableController<INPUT, OUTPUT,
        REQ extends AbstractRequest<Conclusion, ConceptMap, INPUT>,
        REACTIVE_BLOCK extends AbstractReactiveBlock<INPUT, OUTPUT, ?, REACTIVE_BLOCK>,
        CONTROLLER extends ConcludableController<INPUT, OUTPUT, ?, REACTIVE_BLOCK, CONTROLLER>
        > extends AbstractController<ConceptMap, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, CONTROLLER> {

    protected final Map<Conclusion, Driver<ConclusionController>> conclusionControllers;
    protected final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
    protected final Driver<Monitor> monitor;
    protected final Registry registry;
    protected final Concludable concludable;

    public ConcludableController(Driver<CONTROLLER> driver, Concludable concludable,
                                 ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry) {
        super(driver, executorService, registry,
              () -> ConcludableController.class.getSimpleName() + "(pattern: " + concludable + ")");
        this.monitor = monitor;
        this.registry = registry;
        this.concludable = concludable;
        this.conclusionControllers = new HashMap<>();
        this.conclusionUnifiers = new HashMap<>();
    }

    @Override
    public void setUpUpstreamControllers() {
        concludable.getApplicableRules(registry.conceptManager(), registry.logicManager())
                .forEachRemaining(rule -> {
                    Driver<ConclusionController> controller = registry.registerConclusionController(rule.conclusion());
                    conclusionControllers.put(rule.conclusion(), controller);
                    conclusionUnifiers.put(rule.conclusion(), concludable.getUnifiers(rule).toSet());
                });
    }

    @Override
    protected abstract REACTIVE_BLOCK createReactiveBlockFromDriver(Driver<REACTIVE_BLOCK> reactiveBlockDriver, ConceptMap bounds);

    public static class Match extends ConcludableController<Map<Variable, Concept>, ConceptMap,
            ReactiveBlock.Match.Request, ConcludableController.ReactiveBlock.Match, Match> {

        private final Set<Variable.Retrievable> unboundVars;

        public Match(Driver<Match> driver, Concludable concludable,
                     ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry) {
            super(driver, concludable, executorService, monitor, registry);
            this.unboundVars = unboundVars();
        }

        @Override
        protected ReactiveBlock.Match createReactiveBlockFromDriver(Driver<ReactiveBlock.Match> matchDriver,
                                                                    ConceptMap bounds) {
            // TODO: upstreamConclusions contains *all* conclusions even if they are irrelevant for this particular
            //  concludable. They should be filtered before being passed to the concludableReactiveBlock's constructor
            return new ReactiveBlock.Match(
                    matchDriver, driver(), monitor, bounds, unboundVars, conclusionUnifiers,
                    () -> Traversal.traversalIterator(registry, concludable.pattern(), bounds),
                    () -> ReactiveBlock.class.getSimpleName() + "(pattern: " + concludable.pattern() + ", bounds: " + bounds + ")"
            );
        }

        private Set<Variable.Retrievable> unboundVars() {
            Set<Variable.Retrievable> missingBounds = new HashSet<>();
            iterate(concludable.pattern().variables())
                    .filter(var -> var.id().isRetrievable())
                    .forEachRemaining(var -> {
                        if (var.isType() && !var.asType().label().isPresent()) {
                            missingBounds.add(var.asType().id().asRetrievable());
                        } else if (var.isThing() && !var.asThing().iid().isPresent()) {
                            missingBounds.add(var.asThing().id().asRetrievable());
                        }
                    });
            return missingBounds;
        }

        @Override
        public void resolveController(ReactiveBlock.Match.Request req) {
            if (isTerminated()) return;
            // TODO: Should be fixed by generifying Conclusion and then can be pushed up into superclass
            conclusionControllers.get(req.controllerId())
                    .execute(actor -> actor.resolveReactiveBlock(new AbstractReactiveBlock.Connector<>(req.inputId(), req.bounds())));
        }

    }

    protected abstract static class ReactiveBlock<
            INPUT,
            OUTPUT,
            REQ extends AbstractReactiveBlock.Connector.AbstractRequest<?, ?, INPUT>,
            REACTIVE_BLOCK extends AbstractReactiveBlock<INPUT, OUTPUT, REQ, REACTIVE_BLOCK>
            > extends AbstractReactiveBlock<INPUT, OUTPUT, REQ, REACTIVE_BLOCK> {

        private final ConceptMap bounds;
        private final Set<Variable.Retrievable> unboundVars;
        private final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
        private final java.util.function.Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final Set<REQ> requestedConnections;

        public ReactiveBlock(Driver<REACTIVE_BLOCK> driver,
                             Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, ?>> controller,
                             Driver<Monitor> monitor, ConceptMap bounds,
                             Set<Variable.Retrievable> unboundVars,
                             Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                             Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                             Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.bounds = bounds;
            this.unboundVars = unboundVars;
            this.conclusionUnifiers = conclusionUnifiers;
            this.traversalSuppplier = traversalSuppplier;
            this.requestedConnections = new HashSet<>();
        }

        @Override
        public void setUp() {
            setOutputRouter(PoolingStream.fanInFanOut(this));
            // TODO: How do we do a find first optimisation and also know that we're done? This needs to be local to
            //  this reactiveBlock because in general we couldn't call all upstream work done.

            mayAddTraversal();

            conclusionUnifiers.forEach((conclusion, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    Input<INPUT> input = createInput();
                    mayRequestConnection(createRequest(input.identifier(), conclusion, boundsAndRequirements.first()));
                    Stream<ConceptMap, ConceptMap> ununified = input.flatMap(
                            conclusionAns -> unifier.unUnify(
                                    convertToUnunifiable(conclusionAns), boundsAndRequirements.second()
                            )
                    ).buffer();
                    connectToRouter(ununified);
                }));
            });
        }

        protected abstract void connectToRouter(Publisher<ConceptMap> ununified);

        protected abstract REQ createRequest(Reactive.Identifier<INPUT, ?> identifier, Conclusion conclusion,
                                             ConceptMap bounds);

        protected void mayAddTraversal() {
            connectToRouter(Source.create(this, new Operator.Supplier<>(traversalSuppplier)));
        }

        protected abstract Map<Variable, Concept> convertToUnunifiable(INPUT conclusionAns);

        private void mayRequestConnection(REQ conclusionRequest) {
            if (!requestedConnections.contains(conclusionRequest)) {
                requestedConnections.add(conclusionRequest);
                requestConnection(conclusionRequest);
            }
        }

        public static class Match extends ReactiveBlock<Map<Variable, Concept>, ConceptMap, Match.Request, Match> {

            public Match(
                    Driver<Match> driver, Driver<ConcludableController.Match> controller, Driver<Monitor> monitor,
                    ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier, Supplier<String> debugName
            ) {
                super(driver, controller, monitor, bounds, unboundVars, conclusionUnifiers, traversalSuppplier, debugName);
            }

            @Override
            protected void connectToRouter(Publisher<ConceptMap> toConnect) {
                toConnect.registerSubscriber(outputRouter());
            }

            @Override
            protected Request createRequest(Reactive.Identifier<Map<Variable, Concept>, ?> inputId,
                                            Conclusion conclusion, ConceptMap bounds) {
                return new Request(inputId, conclusion, bounds);
            }

            @Override
            protected Map<Variable, Concept> convertToUnunifiable(Map<Variable, Concept> conclusionAns) {
                return conclusionAns;
            }

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, Map<Variable, Concept>> {

                public Request(Reactive.Identifier<Map<Variable, Concept>, ?> inputId, Conclusion controllerId,
                               ConceptMap reactiveBlockId) {
                    super(inputId, controllerId, reactiveBlockId);
                }

            }

        }
    }

}
