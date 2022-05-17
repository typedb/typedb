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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Rule.Conclusion;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.answer.PartialExplanation;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock.Connector.AbstractRequest;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Input;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.RootSink;
import com.vaticle.typedb.core.reasoner.reactive.Source;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ConcludableController<INPUT, OUTPUT,
        REQ extends AbstractRequest<Conclusion, ConceptMap, INPUT>,
        REACTIVE_BLOCK extends ConcludableController.ReactiveBlock<INPUT, OUTPUT, ?, REACTIVE_BLOCK>,
        CONTROLLER extends ConcludableController<INPUT, OUTPUT, ?, REACTIVE_BLOCK, CONTROLLER>
        > extends AbstractController<ConceptMap, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, CONTROLLER> {

    protected final Map<Conclusion, Driver<? extends ConclusionController<INPUT, ?, ?>>> conclusionControllers;
    protected final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
    protected final Concludable concludable;

    public ConcludableController(Driver<CONTROLLER> driver, Concludable concludable, Context context) {
        super(driver, context, () -> ConcludableController.class.getSimpleName() + "(pattern: " + concludable + ")");
        this.concludable = concludable;
        this.conclusionControllers = new HashMap<>();
        this.conclusionUnifiers = new HashMap<>();
    }

    @Override
    public void setUpUpstreamControllers() {
        concludable.getApplicableRules(registry().conceptManager(), registry().logicManager())
                .forEachRemaining(rule -> {
                    Driver<? extends ConclusionController<INPUT, ?, ?>> controller = registerConclusionController(rule);
                    conclusionControllers.put(rule.conclusion(), controller);
                    conclusionUnifiers.put(rule.conclusion(), concludable.getUnifiers(rule).toSet());
                });
    }

    protected abstract Driver<? extends ConclusionController<INPUT, ?, ?>> registerConclusionController(Rule rule);

    @Override
    public void resolveController(REQ req) {
        if (isTerminated()) return;
        conclusionControllers.get(req.controllerId())
                .execute(actor -> actor.resolveReactiveBlock(new AbstractReactiveBlock.Connector<>(req.inputId(), req.bounds())));
    }

    public static class Match extends ConcludableController<Map<Variable, Concept>, ConceptMap,
            ReactiveBlock.Match.Request, ReactiveBlock.Match, Match> {

        private final Set<Variable.Retrievable> unboundVars;

        public Match(Driver<Match> driver, Concludable concludable, Context context) {
            super(driver, concludable, context);
            this.unboundVars = unboundVars();
        }

        @Override
        protected ReactiveBlock.Match createReactiveBlockFromDriver(Driver<ReactiveBlock.Match> matchDriver,
                                                                    ConceptMap bounds) {
            // TODO: upstreamConclusions contains *all* conclusions even if they are irrelevant for this particular
            //  concludable. They should be filtered before being passed to the concludableReactiveBlock's constructor
            return new ReactiveBlock.Match(
                    matchDriver, driver(), concludable, reactiveBlockContext(), bounds, unboundVars, conclusionUnifiers,
                    () -> Traversal.traversalIterator(registry(), concludable.pattern(), bounds),
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
        public Driver<ConclusionController.Match> registerConclusionController(Rule rule) {
            return registry().registerMatchConclusionController(rule.conclusion());
        }

    }

    public static class Explain extends ConcludableController<PartialExplanation, Explanation, ReactiveBlock.Explain.Request, ReactiveBlock.Explain, Explain> {

        private final ConceptMap bounds;
        private final ReasonerConsumer<Explanation> reasonerConsumer;

        public Explain(Driver<Explain> driver, Concludable concludable, ConceptMap bounds, Context context,
                       ReasonerConsumer<Explanation> reasonerConsumer) {
            super(driver, concludable, context);
            this.bounds = bounds;
            this.reasonerConsumer = reasonerConsumer;
        }

        @Override
        public void initialise() {
            super.initialise();
            createReactiveBlockIfAbsent(bounds);
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            reasonerConsumer.exception(cause);
        }

        @Override
        protected ReactiveBlock.Explain createReactiveBlockFromDriver(Driver<ReactiveBlock.Explain> explainDriver,
                                                                      ConceptMap bounds) {
            // TODO: upstreamConclusions contains *all* conclusions even if they are irrelevant for this particular
            //  concludable. They should be filtered before being passed to the concludableReactiveBlock's constructor
            assert bounds.equals(this.bounds);
            return new ReactiveBlock.Explain(
                    explainDriver, driver(), reactiveBlockContext(), bounds, set(), conclusionUnifiers, reasonerConsumer,
                    () -> Traversal.traversalIterator(registry(), concludable.pattern(), bounds),
                    () -> ReactiveBlock.class.getSimpleName() + "(pattern: " + concludable.pattern() + ", bounds: " + bounds + ")"
            );
        }

        @Override
        protected Driver<ConclusionController.Explain> registerConclusionController(Rule rule) {
            return registry().registerExplainConclusionController(rule.conclusion());
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
        private final Set<REQ> requestedConnections;
        protected final java.util.function.Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;

        protected ReactiveBlock(Driver<REACTIVE_BLOCK> driver,
                                Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, ?>> controller,
                                Context context, ConceptMap bounds,
                                Set<Variable.Retrievable> unboundVars,
                                Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                                Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                                Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.bounds = bounds;
            this.unboundVars = unboundVars;
            this.conclusionUnifiers = conclusionUnifiers;
            this.traversalSuppplier = traversalSuppplier;
            this.requestedConnections = new HashSet<>();
        }

        @Override
        public void setUp() {
            setOutputRouter(PoolingStream.fanInFanOut(this));
            // TODO: How do we do a find first optimisation (when unbound vars is empty) and also know that we're done?
            //  This needs to be local to this reactiveBlock because in general we couldn't call all upstream work done.

            mayAddTraversal();

            conclusionUnifiers.forEach((conclusion, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    Input<INPUT> input = createInput();
                    mayRequestConnection(createRequest(input.identifier(), conclusion, boundsAndRequirements.first()));
                    buildOutput(input, unifier, boundsAndRequirements.second()).buffer().registerSubscriber(outputRouter());
                }));
            });
        }

        protected abstract Publisher<OUTPUT> buildOutput(Publisher<INPUT> input, Unifier unifier,
                                                         Unifier.Requirements.Instance requirements);

        protected abstract REQ createRequest(Reactive.Identifier<INPUT, ?> identifier, Conclusion conclusion,
                                             ConceptMap bounds);

        protected abstract void mayAddTraversal();

        private void mayRequestConnection(REQ conclusionRequest) {
            if (!requestedConnections.contains(conclusionRequest)) {
                requestedConnections.add(conclusionRequest);
                requestConnection(conclusionRequest);
            }
        }

        public static class Match extends ReactiveBlock<Map<Variable, Concept>, ConceptMap, Match.Request, Match> {

            private final Concludable concludable;

            public Match(
                    Driver<Match> driver, Driver<ConcludableController.Match> controller, Concludable concludable,
                    Context context, ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier, Supplier<String> debugName
            ) {
                super(driver, controller, context, bounds, unboundVars, conclusionUnifiers, traversalSuppplier,
                      debugName);
                this.concludable = concludable;
            }

            @Override
            protected void mayAddTraversal() {
                Source.create(this, new Operator.Supplier<>(traversalSuppplier))
                        .flatMap(this::filterInferred)
                        .registerSubscriber(outputRouter());
            }

            private FunctionalIterator<ConceptMap> filterInferred(ConceptMap conceptMap) {
                // TODO: Requires duplicate effort to filter out inferred concludable answers. Instead create a new
                //  traversal mode that doesn't return inferred concepts
                if (concludable.isInferredAnswer(conceptMap)) return Iterators.empty();
                for (Concept c : conceptMap.concepts().values()) {
                    if (c.isThing() && c.asThing().isInferred()) return Iterators.empty();
                }
                return Iterators.single(conceptMap);
            }

            @Override
            protected Publisher<ConceptMap> buildOutput(Publisher<Map<Variable, Concept>> input,
                                                        Unifier unifier,
                                                        Unifier.Requirements.Instance requirements) {
                return input.flatMap(conclusionAns -> unifier.unUnify(conclusionAns, requirements))
                        .map(ans -> withExplainable(ans, concludable));
            }

            protected static ConceptMap withExplainable(ConceptMap conceptMap, Concludable concludable) {
                if (concludable.isRelation() || concludable.isAttribute() || concludable.isIsa()) {
                    return conceptMap.withExplainableConcept(concludable.generating().get().id(), concludable.pattern());
                } else if (concludable.isHas()) {
                    return conceptMap.withExplainableAttrOwnership(
                            concludable.asHas().owner().id(), concludable.asHas().attribute().id(), concludable.pattern()
                    );
                } else {
                    throw TypeDBException.of(ILLEGAL_STATE);
                }
            }

            @Override
            protected Request createRequest(Reactive.Identifier<Map<Variable, Concept>, ?> inputId,
                                            Conclusion conclusion, ConceptMap bounds) {
                return new Request(inputId, conclusion, bounds);
            }

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, Map<Variable, Concept>> {

                public Request(Reactive.Identifier<Map<Variable, Concept>, ?> inputId, Conclusion controllerId,
                               ConceptMap reactiveBlockId) {
                    super(inputId, controllerId, reactiveBlockId);
                }

            }

        }

        public static class Explain extends ReactiveBlock<PartialExplanation, Explanation, Explain.Request, Explain> {

            private final ReasonerConsumer<Explanation> reasonerConsumer;
            private RootSink<Explanation> rootSink;

            public Explain(
                    Driver<Explain> driver, Driver<ConcludableController.Explain> controller, Context context,
                    ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                    ReasonerConsumer<Explanation> reasonerConsumer,
                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                    Supplier<String> debugName
            ) {
                super(driver, controller, context, bounds, unboundVars, conclusionUnifiers, traversalSuppplier,
                      debugName);
                this.reasonerConsumer = reasonerConsumer;
            }

            @Override
            public void setUp() {
                super.setUp();
                rootSink = new RootSink<>(this, reasonerConsumer);
                outputRouter().registerSubscriber(rootSink);
            }

            @Override
            public void rootPull() {
                rootSink.pull();
            }

            @Override
            protected Publisher<Explanation> buildOutput(Publisher<PartialExplanation> input,
                                                         Unifier unifier,
                                                         Unifier.Requirements.Instance requirements) {
                return input.map(
                        p -> new Explanation(p.rule(), unifier.mapping(), p.conclusionAnswer(), p.conditionAnswer())
                );
            }

            @Override
            protected void mayAddTraversal() {
                // No traversal when explaining
            }


            @Override
            public void onFinished(Reactive.Identifier<?, ?> finishable) {
                assert finishable == rootSink.identifier();
                rootSink.finished();
            }

            @Override
            protected Explain.Request createRequest(Reactive.Identifier<PartialExplanation, ?> inputId,
                                                    Conclusion conclusion, ConceptMap bounds) {
                return new Request(inputId, conclusion, bounds);
            }

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, PartialExplanation> {

                protected Request(Reactive.Identifier<PartialExplanation, ?> inputId, Conclusion conclusion,
                                  ConceptMap conceptMap) {
                    super(inputId, conclusion, conceptMap);
                }
            }
        }
    }

}
