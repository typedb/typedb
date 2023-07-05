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

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Materialiser.Materialisation;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisable;
import com.vaticle.typedb.core.reasoner.answer.PartialExplanation;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController.Request.ConditionRequest;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController.Request.MaterialiserRequest;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest.Identifier;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanOut;

public abstract class ConclusionController<
        OUTPUT, PROCESSOR extends AbstractProcessor<Either<ConceptMap, Materialisation>, OUTPUT, ?, PROCESSOR>,
        CONTROLLER extends ConclusionController<OUTPUT, PROCESSOR, CONTROLLER>
        > extends AbstractController<
        ConceptMap, Either<ConceptMap, Materialisation>, OUTPUT, ConclusionController.Request<?, ?>,
        PROCESSOR, CONTROLLER
        > {

    final Rule.Conclusion conclusion;
    private final Driver<MaterialisationController> materialisationController;
    private Driver<ConditionController> conditionController;

    private ConclusionController(Driver<CONTROLLER> driver, Rule.Conclusion conclusion,
                                 Driver<MaterialisationController> materialisationController, Context context) {
        super(driver, context, () -> ConclusionController.class.getSimpleName() + "(pattern: " + conclusion + ")");
        this.conclusion = conclusion;
        this.materialisationController = materialisationController;
    }

    @Override
    public void setUpUpstreamControllers() {
        conditionController = registry().getOrCreateCondition(conclusion.rule().condition());
    }

    @Override
    public void routeConnectionRequest(Request<?, ?> req) {
        if (req.isCondition()) {
            conditionController.execute(actor -> actor.establishProcessorConnection(req.asCondition()));
        } else if (req.isMaterialiser()) {
            materialisationController.execute(actor -> actor.establishProcessorConnection(req.asMaterialiser()));
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    public static class Match extends ConclusionController<Map<Variable, Concept>, Processor.Match, Match> {

        public Match(Driver<Match> driver, Rule.Conclusion conclusion,
                     Driver<MaterialisationController> materialisationController, Context context) {
            super(driver, conclusion, materialisationController, context);
        }

        @Override
        protected Processor.Match createProcessorFromDriver(Driver<Processor.Match> processorDriver,
                                                            ConceptMap bounds) {
            return new Processor.Match(
                    processorDriver, driver(), processorContext(), conclusion.rule(), bounds,
                    registry().conceptManager(),
                    () -> Processor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
            );
        }

    }

    public static class Explain extends ConclusionController<PartialExplanation, Processor.Explain, Explain> {

        Explain(Driver<Explain> driver, Rule.Conclusion conclusion,
                Driver<MaterialisationController> materialisationController, Context context) {
            super(driver, conclusion, materialisationController, context);
        }

        @Override
        protected Processor.Explain createProcessorFromDriver(Driver<Processor.Explain> processorDriver,
                                                              ConceptMap bounds) {
            return new Processor.Explain(
                    processorDriver, driver(), processorContext(), conclusion.rule(), bounds,
                    registry().conceptManager(),
                    () -> Processor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
            );
        }
    }

    protected static class Request<CONTROLLER_ID, BOUNDS>
            extends AbstractRequest<CONTROLLER_ID, BOUNDS, Either<ConceptMap, Materialisation>> {

        Request(Reactive.Identifier inputPortId,
                Driver<? extends Processor<?, ?>> inputPortProcessor, CONTROLLER_ID controller_id, BOUNDS bounds) {
            super(inputPortId, inputPortProcessor, controller_id, bounds);
        }

        boolean isCondition() {
            return false;
        }

        ConditionRequest asCondition() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        boolean isMaterialiser() {
            return false;
        }

        MaterialiserRequest asMaterialiser() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        protected static class ConditionRequest extends Request<Rule.Condition, ConceptMap> {

            ConditionRequest(Reactive.Identifier inputPortId,
                             Driver<? extends Processor<?, ?>> inputPortProcessor,
                             Rule.Condition controllerId, ConceptMap processorId) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

            @Override
            public boolean isCondition() {
                return true;
            }

            @Override
            public ConditionRequest asCondition() {
                return this;
            }

        }

        protected static class MaterialiserRequest extends Request<Void, Materialisable> {

            MaterialiserRequest(
                    Reactive.Identifier inputPortId,
                    Driver<? extends Processor<?, ?>> inputPortProcessor, Void controllerId, Materialisable processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

            @Override
            public boolean isMaterialiser() {
                return true;
            }

            @Override
            public MaterialiserRequest asMaterialiser() {
                return this;
            }

        }
    }

    protected abstract static class Processor<OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>>
            extends AbstractProcessor<Either<ConceptMap, Materialisation>, OUTPUT, Request<?, ?>, PROCESSOR> {

        private final Rule rule;
        private final ConceptMap bounds;
        private final ConceptManager conceptManager;
        private final Set<Identifier> conditionRequests;
        private final Set<Identifier> materialisationRequests;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends ConclusionController<OUTPUT, PROCESSOR, ?>> controller,
                  Context context, Rule rule, ConceptMap bounds, ConceptManager conceptManager,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.rule = rule;
            this.bounds = bounds;
            this.conceptManager = conceptManager;
            this.conditionRequests = new HashSet<>();
            this.materialisationRequests = new HashSet<>();
        }

        private Rule rule() {
            return rule;
        }

        private ConceptMap bounds() {
            return bounds;
        }

        private ConceptManager conceptManager() {
            return conceptManager;
        }

        @Override
        public void setUp() {
            setHubReactive(fanOut(this));
            InputPort<Either<ConceptMap, Materialisation>> conditionInput = createInputPort();
            ConceptMap filteredBounds = bounds().filter(rule.when().sharedVariables());
            mayRequestCondition(new ConditionRequest(conditionInput.identifier(), driver(), rule.condition(), filteredBounds));
            Stream<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> conclusionReactive = createStream();
            conditionInput.map(Processor::convertConclusionInput).registerSubscriber(conclusionReactive);
            conclusionReactive.registerSubscriber(hubReactive());
        }

        protected abstract Stream<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> createStream();

        private static Either<ConceptMap, Map<Variable, Concept>> convertConclusionInput(Either<ConceptMap, Materialisation> input) {
            return Either.first(input.first());
        }

        private void mayRequestCondition(ConditionRequest conditionRequest) {
            if (!conditionRequests.contains(conditionRequest.id())) {
                conditionRequests.add(conditionRequest.id());
                requestConnection(conditionRequest);
            }
        }

        private void mayRequestMaterialiser(MaterialiserRequest materialisationRequest) {
            if (!materialisationRequests.contains(materialisationRequest.id())) {
                materialisationRequests.add(materialisationRequest.id());
                requestConnection(materialisationRequest);
            }
        }

        protected static class Match extends Processor<Map<Variable, Concept>, Match> {

            Match(Driver<Match> driver, Driver<ConclusionController.Match> controller, Context context,
                  Rule rule, ConceptMap bounds, ConceptManager conceptManager, Supplier<String> debugName) {
                super(driver, controller, context, rule, bounds, conceptManager, debugName);
            }

            @Override
            protected Stream<Either<ConceptMap, Map<Variable, Concept>>, Map<Variable, Concept>> createStream() {
                return new ConclusionStream.Match(this);
            }
        }

        protected static class Explain extends Processor<PartialExplanation, Explain> {

            Explain(Driver<Explain> driver,
                    Driver<? extends ConclusionController<PartialExplanation, Explain, ?>> controller,
                    Context context, Rule rule, ConceptMap bounds, ConceptManager conceptManager,
                    Supplier<String> debugName) {
                super(driver, controller, context, rule, bounds, conceptManager, debugName);
            }

            @Override
            protected Stream<Either<ConceptMap, Map<Variable, Concept>>, PartialExplanation> createStream() {
                return new ConclusionStream.Explain(this);
            }

        }

        private static abstract class ConclusionStream<OUTPUT> extends TransformationStream<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> {

            private final Processor<?, ?> processor;

            private ConclusionStream(Processor<?, ?> processor) {
                super(processor, new SubscriberRegistry.Single<>(), new PublisherRegistry.Multi<>());
                this.processor = processor;
            }

            Processor<?, ?> conclusionProcessor() {
                // TODO: should use processor() from superclass instead, but here we require specific methods available
                //  on conclusion processor only.
                return processor;
            }

            @Override
            public Either<Publisher<Either<ConceptMap, Map<Variable, Concept>>>, Set<OUTPUT>> accept(
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher,
                    Either<ConceptMap, Map<Variable, Concept>> packet
            ) {
                if (packet.isFirst()) {
                    assert packet.first().concepts().keySet().containsAll(Collections.intersection(conclusionProcessor().rule().conclusion().pattern().retrieves(), conclusionProcessor().rule().condition().disjunction().pattern().sharedVariables()));
                    InputPort<Either<ConceptMap, Materialisation>> materialisationInput = conclusionProcessor().createInputPort();
                    ConceptMap filteredConditionAns = packet.first().filter(conclusionProcessor().rule().conclusion().retrievableIds());
                    conclusionProcessor().mayRequestMaterialiser(new MaterialiserRequest(
                            materialisationInput.identifier(), conclusionProcessor().driver(), null,
                            conclusionProcessor().rule().conclusion().materialisable(filteredConditionAns, conclusionProcessor().conceptManager()))
                    );
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> op = materialisationInput
                            .map(m -> m.second().bindToConclusion(conclusionProcessor().rule().conclusion(), filteredConditionAns))
                            .flatMap(m -> merge(m, conclusionProcessor().bounds()))
                            .map(Either::second);
                    mayStoreConditionAnswer(op, packet.first());
                    return Either.first(op);
                } else {
                    return Either.second(set(packageAnswer(publisher, packet.second())));
                }
            }

            private static FunctionalIterator<Map<Variable, Concept>> merge(Map<Variable, Concept> materialisation, ConceptMap bounds) {
                for (Map.Entry<Variable, Concept> entry : materialisation.entrySet()) {
                    Variable v = entry.getKey();
                    Concept c = entry.getValue();
                    if (v.isRetrievable() && bounds.contains(v.asRetrievable())
                            && !bounds.get(v.asRetrievable()).equals(c)) {
                        return Iterators.empty();
                    }
                }
                return Iterators.single(materialisation);
            }

            protected abstract void mayStoreConditionAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput, ConceptMap conditionAnswer);

            protected abstract OUTPUT packageAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher, Map<Variable, Concept> conclusionAnswer);

            protected static class Match extends ConclusionStream<Map<Variable, Concept>> {

                private Match(Processor<?, ?> processor) {
                    super(processor);
                }

                @Override
                protected void mayStoreConditionAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput, ConceptMap conditionAnswer) {}

                @Override
                protected Map<Variable, Concept> packageAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher, Map<Variable, Concept> conclusionAnswer) {
                    return conclusionAnswer;
                }
            }

            protected static class Explain extends ConclusionStream<PartialExplanation> {

                private final Map<Publisher<Either<ConceptMap, Map<Variable, Concept>>>, ConceptMap> conditionAnswers;

                private Explain(Processor<?, ?> processor) {
                    super(processor);
                    this.conditionAnswers = new HashMap<>();
                }

                @Override
                protected void mayStoreConditionAnswer(
                        Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput,
                        ConceptMap conditionAnswer
                ) {
                    conditionAnswers.put(materialisationInput, conditionAnswer);
                }

                @Override
                protected PartialExplanation packageAnswer(
                        Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialiserInput,
                        Map<Variable, Concept> conclusionAnswer
                ) {
                    return PartialExplanation.create(conclusionProcessor().rule(), conclusionAnswer, conditionAnswers.get(materialiserInput));
                }
            }

        }

    }

}
