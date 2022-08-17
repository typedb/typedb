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
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest.Identifier;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.RootSink;
import com.vaticle.typedb.core.reasoner.processor.reactive.Source;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanInFanOut;

public abstract class ConcludableController<INPUT, OUTPUT,
        REQ extends AbstractRequest<Conclusion, ConceptMap, INPUT, ?>,
        PROCESSOR extends ConcludableController.Processor<INPUT, OUTPUT, ?, PROCESSOR>,
        CONTROLLER extends ConcludableController<INPUT, OUTPUT, ?, PROCESSOR, CONTROLLER>
        > extends AbstractController<ConceptMap, INPUT, OUTPUT, REQ, PROCESSOR, CONTROLLER> {

    private final Map<Conclusion, Driver<? extends ConclusionController<INPUT, ?, ?>>> conclusionControllers;
    final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
    final Concludable concludable;

    private ConcludableController(Driver<CONTROLLER> driver, Concludable concludable, Context context) {
        super(driver, context, () -> ConcludableController.class.getSimpleName() + "(pattern: " + concludable + ")");
        this.concludable = concludable;
        this.conclusionControllers = new HashMap<>();
        this.conclusionUnifiers = new HashMap<>();
    }

    @Override
    public void setUpUpstreamControllers() {
        iterate(registry().logicManager().applicableRules(concludable).entrySet()).forEachRemaining( ruleAndUnifiers -> {
                Rule rule = ruleAndUnifiers.getKey();
                Driver<? extends ConclusionController<INPUT, ?, ?>> controller = registerConclusionController(rule);
                conclusionControllers.put(rule.conclusion(), controller);
                conclusionUnifiers.put(rule.conclusion(), ruleAndUnifiers.getValue());
        });
    }

    protected abstract Driver<? extends ConclusionController<INPUT, ?, ?>> registerConclusionController(Rule rule);

    @Override
    public void routeConnectionRequest(REQ req) {
        if (isTerminated()) return;
        conclusionControllers.get(req.controllerId()).execute(actor -> actor.establishProcessorConnection(req));
    }

    public static class Match extends ConcludableController<Map<Variable, Concept>, ConceptMap, Processor.Match.Request,
            Processor.Match, Match> {

        private final Set<Variable.Retrievable> unboundVars;

        public Match(Driver<Match> driver, Concludable concludable, Context context) {
            super(driver, concludable, context);
            this.unboundVars = unboundVars();
        }

        @Override
        protected Processor.Match createProcessorFromDriver(Driver<Processor.Match> matchDriver, ConceptMap bounds) {
            return new Processor.Match(
                    matchDriver, driver(), concludable, processorContext(), bounds, unboundVars, conclusionUnifiers,
                    () -> Traversal.traversalIterator(registry(), concludable.pattern(), bounds),
                    () -> Processor.class.getSimpleName() + "(pattern: " + concludable.pattern() + ", bounds: " + bounds + ")"
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
            return registry().getOrCreateMatchConclusion(rule.conclusion());
        }

    }

    public static class Explain extends ConcludableController<PartialExplanation, Explanation, Processor.Explain.Request, Processor.Explain, Explain> {

        private final ConceptMap bounds;
        private final ReasonerConsumer<Explanation> reasonerConsumer;

        Explain(Driver<Explain> driver, Concludable concludable, ConceptMap bounds, Context context,
                ReasonerConsumer<Explanation> reasonerConsumer) {
            super(driver, concludable, context);
            this.bounds = bounds;
            this.reasonerConsumer = reasonerConsumer;
        }

        @Override
        public void initialise() {
            super.initialise();
            getOrCreateProcessor(bounds);
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            reasonerConsumer.exception(cause);
        }

        @Override
        protected Processor.Explain createProcessorFromDriver(Driver<Processor.Explain> explainDriver,
                                                                      ConceptMap bounds) {
            // TODO: upstreamConclusions contains *all* conclusions even if they are irrelevant for this particular
            //  concludable. They should be filtered before being passed to the concludableProcessor's constructor
            assert bounds.equals(this.bounds);
            return new Processor.Explain(
                    explainDriver, driver(), processorContext(), concludable, bounds, set(), conclusionUnifiers, reasonerConsumer,
                    () -> Traversal.traversalIterator(registry(), concludable.pattern(), bounds),
                    () -> Processor.class.getSimpleName() + "(pattern: " + concludable.pattern() + ", bounds: " + bounds + ")"
            );
        }

        @Override
        protected Driver<ConclusionController.Explain> registerConclusionController(Rule rule) {
            return registry().getOrCreateExplainConclusion(rule.conclusion());
        }
    }

    protected abstract static class Processor<
            INPUT, OUTPUT, REQ extends AbstractRequest<?, ?, INPUT, ?>,
            PROCESSOR extends AbstractProcessor<INPUT, OUTPUT, REQ, PROCESSOR>
            > extends AbstractProcessor<INPUT, OUTPUT, REQ, PROCESSOR> {

        private final ConceptMap bounds;
        private final Set<Variable.Retrievable> unboundVars;  // TODO: Can just use a boolean to indicate if fully bound
        private final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
        private final Set<Identifier> requestedConnections;
        final java.util.function.Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller,
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
            setHubReactive(fanInFanOut(this));
            // TODO: Add a find first optimisation when all variables are bound
            mayAddTraversal();
            conclusionUnifiers.forEach((conclusion, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    InputPort<INPUT> inputPort = createInputPort();
                    mayRequestConnection(createRequest(inputPort.identifier(), conclusion, boundsAndRequirements.first()));
                    transformInput(inputPort, unifier, boundsAndRequirements.second()).flatMap(this::rejectMismatchedInference).buffer().registerSubscriber(hubReactive());
                }));
            });
        }

        /*
         * This method rectifies a design issue: it is possible for an `Isa` or `Attribute` concludable to unify with a
         * `Conclusion.Has.Explicit` rule. In the case where the `has` edge is inferred but the owned attribute is not,
         * the concludable's processor will receive answers where the concept it is concerned with is non-inferred. This
         * leads to an extra invalid attribute explainable (and its explanation). Instead, we reject these
         * non-inferred answers.
         */
        protected abstract FunctionalIterator<OUTPUT> rejectMismatchedInference(OUTPUT output);

        protected abstract Publisher<OUTPUT> transformInput(Publisher<INPUT> input, Unifier unifier,
                                                            Unifier.Requirements.Instance requirements);

        protected abstract REQ createRequest(Reactive.Identifier identifier, Conclusion conclusion,
                                             ConceptMap bounds);

        protected abstract void mayAddTraversal();

        private void mayRequestConnection(REQ conclusionRequest) {
            if (!requestedConnections.contains(conclusionRequest.id())) {
                requestedConnections.add(conclusionRequest.id());
                requestConnection(conclusionRequest);
            }
        }

        public static class Match extends Processor<Map<Variable, Concept>, ConceptMap, Match.Request, Match> {

            private final Concludable concludable;

            Match(
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
                new Source<>(this, traversalSuppplier).flatMap(this::filterInferred).registerSubscriber(hubReactive());
            }

            private FunctionalIterator<ConceptMap> filterInferred(ConceptMap conceptMap) {
                // TODO: Requires duplicate effort to filter out inferred concludable answers. Instead create a new
                //  traversal mode that doesn't return inferred concepts
                if (concludable.isInferredAnswer(conceptMap)) return empty();
                for (Concept c : conceptMap.concepts().values()) {
                    if (c.isThing() && c.asThing().isInferred()) return empty();
                }
                return Iterators.single(conceptMap);
            }

            @Override
            protected FunctionalIterator<ConceptMap> rejectMismatchedInference(ConceptMap conceptMap) {
                Concept conceptToCheck = null;
                if (concludable.isAttribute()) {
                    conceptToCheck = conceptMap.get(concludable.asAttribute().attribute().id());
                } else if (concludable.isIsa()) {
                    conceptToCheck = conceptMap.get(concludable.asIsa().isa().owner().id());
                }
                if (conceptToCheck != null) {
                    if (conceptToCheck.asThing().isInferred()) {
                        return Iterators.single(conceptMap);
                    } else {
                        return empty();
                    }
                }
                return Iterators.single(conceptMap);
            }

            @Override
            protected Publisher<ConceptMap> transformInput(Publisher<Map<Variable, Concept>> input,
                                                           Unifier unifier,
                                                           Unifier.Requirements.Instance requirements) {
                return input.flatMap(conclusionAns -> unifier.unUnify(conclusionAns, requirements))
                        .map(ans -> withExplainable(ans, concludable));
            }

            static ConceptMap withExplainable(ConceptMap conceptMap, Concludable concludable) {
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
            protected Request createRequest(Reactive.Identifier inputPortId,
                                            Conclusion conclusion, ConceptMap bounds) {
                return new Request(inputPortId, driver(), conclusion, bounds);
            }

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, Map<Variable, Concept>, ConclusionController.Match> {

                Request(Reactive.Identifier inputPortId,
                        Driver<Match> inputPortProcessor, Conclusion controllerId, ConceptMap processorId) {
                    super(inputPortId, inputPortProcessor, controllerId, processorId);
                }

            }

        }

        public static class Explain extends Processor<PartialExplanation, Explanation, Explain.Request, Explain> {

            private final ReasonerConsumer<Explanation> reasonerConsumer;
            private final Concludable concludable;
            private RootSink<Explanation> rootSink;

            Explain(
                    Driver<Explain> driver, Driver<ConcludableController.Explain> controller, Context context,
                    Concludable concludable,
                    ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                    ReasonerConsumer<Explanation> reasonerConsumer,
                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                    Supplier<String> debugName
            ) {
                super(driver, controller, context, bounds, unboundVars, conclusionUnifiers, traversalSuppplier,
                      debugName);
                this.concludable = concludable;
                this.reasonerConsumer = reasonerConsumer;
            }

            @Override
            public void setUp() {
                super.setUp();
                rootSink = new RootSink<>(this, reasonerConsumer);
                hubReactive().registerSubscriber(rootSink);
            }

            @Override
            protected FunctionalIterator<Explanation> rejectMismatchedInference(Explanation explanation) {
                Set<Variable> conclusionConceptsToCheck = null;
                if (concludable.isAttribute()) {
                    conclusionConceptsToCheck = explanation.variableMapping().get(concludable.asAttribute().attribute().id());
                } else if (concludable.isIsa()) {
                    conclusionConceptsToCheck = explanation.variableMapping().get(concludable.asIsa().isa().owner().id());
                }
                if (conclusionConceptsToCheck != null) {
                    for (Variable toCheck : conclusionConceptsToCheck) {
                        if (explanation.conclusionAnswer().concepts().get(toCheck).asThing().isInferred()) {
                            return Iterators.single(explanation);
                        } else {
                            return empty();
                        }
                    }
                }
                return Iterators.single(explanation);
            }

            @Override
            public void rootPull() {
                rootSink.pull();
            }

            @Override
            protected Publisher<Explanation> transformInput(Publisher<PartialExplanation> input,
                                                            Unifier unifier,
                                                            Unifier.Requirements.Instance requirements) {
                return input.flatMap(p -> {
                    FunctionalIterator<ConceptMap> exists = unifier.unUnify(p.conclusionAnswer().concepts(), requirements);
                    if (exists.hasNext()) return iterate(p);
                    else return empty();
                }).map(
                        p -> new Explanation(p.rule(), unifier.mapping(), p.conclusionAnswer(), p.conditionAnswer())
                );
            }

            @Override
            protected void mayAddTraversal() {
                // No traversal when explaining
            }


            @Override
            public void onFinished(Reactive.Identifier finishable) {
                assert finishable == rootSink.identifier();
                rootSink.finished();
            }

            @Override
            protected Explain.Request createRequest(Reactive.Identifier inputPortId,
                                                    Conclusion conclusion, ConceptMap bounds) {
                return new Request(inputPortId, driver(), conclusion, bounds);
            }

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, PartialExplanation, ConclusionController.Explain> {

                Request(Reactive.Identifier inputPortId,
                        Driver<Explain> inputPortProcessor, Conclusion conclusion, ConceptMap conceptMap) {
                    super(inputPortId, inputPortProcessor, conclusion, conceptMap);
                }
            }
        }
    }

}
