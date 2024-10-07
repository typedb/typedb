/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Concept.Existence.INFERRED;
import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanInFanOut;

public abstract class ConcludableController<INPUT, OUTPUT,
        REQ extends AbstractRequest<Conclusion, ConceptMap, INPUT>,
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

    public static boolean canBypassReasoning(Concludable concludable, Set<com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable> boundVariables, boolean isExplainEnabled) {
        return !isExplainEnabled && !concludable.isHas() && boundVariables.contains(concludable.generatingVariable().id());
    }

    @Override
    public void setUpUpstreamControllers() {
        iterate(registry().logicManager().applicableRules(concludable).entrySet()).forEachRemaining(ruleAndUnifiers -> {
                Rule rule = ruleAndUnifiers.getKey();
                Driver<? extends ConclusionController<INPUT, ?, ?>> controller = registerConclusionController(rule);
                conclusionControllers.put(rule.conclusion(), controller);
                conclusionUnifiers.put(rule.conclusion(), ruleAndUnifiers.getValue());
        });
    }

    protected abstract Driver<? extends ConclusionController<INPUT, ?, ?>> registerConclusionController(Rule rule);

    @Override
    public void routeConnectionRequest(REQ req) {
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
            Set<com.vaticle.typedb.core.pattern.variable.Variable> boundVariables = Iterators.iterate(bounds.concepts().keySet())
                    .map(id -> concludable.pattern().variable(id))
                    .filter(v -> v != null).toSet();
            planner().planExplainableRoot(concludable, boundVariables);
            setUpUpstreamControllers();
            getOrCreateProcessor(bounds);
        }

        @Override
        public void terminate(@Nullable Throwable cause) {
            super.terminate(cause);
            if (cause != null) {
                reasonerConsumer.exception(cause);
            }
        }

        @Override
        protected Processor.Explain createProcessorFromDriver(Driver<Processor.Explain> explainDriver,
                                                                      ConceptMap bounds) {
            // TODO: upstreamConclusions contains *all* conclusions even if they are irrelevant for this particular
            //  concludable. They should be filtered before being passed to the concludableProcessor's constructor
            assert bounds.equals(this.bounds);
            return new Processor.Explain(
                    explainDriver, driver(), processorContext(), concludable, bounds, conclusionUnifiers, reasonerConsumer,
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
            INPUT, OUTPUT, REQ extends AbstractRequest<?, ?, INPUT>,
            PROCESSOR extends AbstractProcessor<INPUT, OUTPUT, REQ, PROCESSOR>
            > extends AbstractProcessor<INPUT, OUTPUT, REQ, PROCESSOR> {

        final Concludable concludable;
        final ConceptMap bounds;
        private final Map<Conclusion, Set<Unifier>> conclusionUnifiers;
        private final Set<Identifier> requestedConnections;
        final java.util.function.Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller,
                  Concludable concludable, Context context, ConceptMap bounds,
                  Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                  Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.concludable = concludable;
            this.bounds = bounds;
            this.conclusionUnifiers = conclusionUnifiers;
            this.traversalSuppplier = traversalSuppplier;
            this.requestedConnections = new HashSet<>();
        }

        void addRules() {
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


        private void mayRequestConnection(REQ conclusionRequest) {
            if (!requestedConnections.contains(conclusionRequest.id())) {
                requestedConnections.add(conclusionRequest.id());
                requestConnection(conclusionRequest);
            }
        }

        public static class Match extends Processor<Map<Variable, Concept>, ConceptMap, Match.Request, Match> {

            Match(
                    Driver<Match> driver, Driver<ConcludableController.Match> controller, Concludable concludable,
                    Context context, ConceptMap bounds, Set<Variable.Retrievable> unboundVars,
                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier, Supplier<String> debugName
            ) {
                super(driver, controller, concludable, context, bounds, conclusionUnifiers, traversalSuppplier, debugName);
            }


            @Override
            public void setUp() {
                setHubReactive(fanInFanOut(this));
                if (canBypassReasoning(bounds)) {
                    addTraversal(true);
                } else {
                    addTraversal(false);
                    addRules();
                }
            }

            private boolean canBypassReasoning(ConceptMap bounds) {
                if (concludable.isHas()) {
                    Concept owner; Concept attribute;
                    return  (owner = bounds.get(concludable.asHas().owner().id())) != null &&
                            (attribute = bounds.get(concludable.asHas().attribute().id())) != null &&
                            (owner.asThing().hasInferred(attribute.asAttribute()) || owner.asThing().hasNonInferred(attribute.asAttribute()));
                } else {
                    return ConcludableController.canBypassReasoning(concludable, bounds.concepts().keySet(), false);
                }
            }

            protected void addTraversal(boolean includeInferred) {
                if (includeInferred) {
                    new Source<>(this, traversalSuppplier).map(ans -> withExplainable(ans, concludable)).registerSubscriber(hubReactive());
                } else {
                    new Source<>(this, traversalSuppplier).flatMap(this::filterInferred).registerSubscriber(hubReactive());
                }
            }

            private FunctionalIterator<ConceptMap> filterInferred(ConceptMap conceptMap) {
                // TODO: Requires duplicate effort to filter out inferred concludable answers. Instead create a new
                //  traversal mode that doesn't return inferred concepts
                if (concludable.isInferredAnswer(conceptMap)) return empty();
                for (Concept c : conceptMap.concepts().values()) {
                    if (c.isThing() && c.asThing().existence() == INFERRED) return empty();
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
                    if (conceptToCheck.asThing().existence() == INFERRED) {
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
                    return conceptMap.withExplainableConcept(concludable.generatingVariable().id(), concludable.pattern());
                } else if (concludable.isHas()) {
                    return conceptMap.withExplainableOwnership(
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

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, Map<Variable, Concept>> {

                Request(Reactive.Identifier inputPortId,
                        Driver<Match> inputPortProcessor, Conclusion controllerId, ConceptMap processorId) {
                    super(inputPortId, inputPortProcessor, controllerId, processorId);
                }

            }

        }

        public static class Explain extends Processor<PartialExplanation, Explanation, Explain.Request, Explain> {

            private final ReasonerConsumer<Explanation> reasonerConsumer;
            private RootSink<Explanation> rootSink;

            Explain(
                    Driver<Explain> driver, Driver<ConcludableController.Explain> controller, Context context,
                    Concludable concludable,
                    ConceptMap bounds,
                    Map<Conclusion, Set<Unifier>> conclusionUnifiers,
                    ReasonerConsumer<Explanation> reasonerConsumer,
                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                    Supplier<String> debugName
            ) {
                super(driver, controller, concludable, context, bounds, conclusionUnifiers, traversalSuppplier,
                      debugName);
                this.reasonerConsumer = reasonerConsumer;
            }

            @Override
            public void setUp() {
                setHubReactive(fanInFanOut(this));
                addRules();
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
                        switch (explanation.conclusionAnswer().concepts().get(toCheck).asThing().existence()) {
                            case STORED: return empty();
                            case INFERRED: return Iterators.single(explanation);
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
            public void onFinished(Reactive.Identifier finishable) {
                assert finishable == rootSink.identifier();
                rootSink.finished();
            }

            @Override
            protected Explain.Request createRequest(Reactive.Identifier inputPortId,
                                                    Conclusion conclusion, ConceptMap bounds) {
                return new Request(inputPortId, driver(), conclusion, bounds);
            }

            protected static class Request extends AbstractRequest<Conclusion, ConceptMap, PartialExplanation> {

                Request(Reactive.Identifier inputPortId,
                        Driver<Explain> inputPortProcessor, Conclusion conclusion, ConceptMap conceptMap) {
                    super(inputPortId, inputPortProcessor, conclusion, conceptMap);
                }
            }
        }
    }

}
