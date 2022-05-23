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
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream.fanIn;

public abstract class ConclusionController<
        OUTPUT, PROCESSOR extends AbstractProcessor<Either<ConceptMap, Materialisation>, OUTPUT, ?, PROCESSOR>,
        CONTROLLER extends ConclusionController<OUTPUT, PROCESSOR, CONTROLLER>
        > extends AbstractController<
        ConceptMap, Either<ConceptMap, Materialisation>, OUTPUT, ConclusionController.Request<?, ?, ?>,
        PROCESSOR, CONTROLLER
        > {

    protected final Rule.Conclusion conclusion;
    protected final Driver<MaterialisationController> materialisationController;
    protected Driver<ConditionController> conditionController;

    public ConclusionController(Driver<CONTROLLER> driver, Rule.Conclusion conclusion,
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
    public void routeConnectionRequest(Request<?, ?, ?> req) {
        if (isTerminated()) return;
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

        public Explain(Driver<Explain> driver, Rule.Conclusion conclusion,
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

    protected static class Request<
            CONTROLLER_ID, BOUNDS,
            CONTROLLER extends AbstractController<BOUNDS, ?, Either<ConceptMap, Materialisation>, ?, ?, ?>
            > extends AbstractRequest<CONTROLLER_ID, BOUNDS, Either<ConceptMap, Materialisation>, CONTROLLER> {

        protected Request(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputPortId,
                          CONTROLLER_ID controller_id, BOUNDS bounds) {
            super(inputPortId, controller_id, bounds);
        }

        public boolean isCondition() {
            return false;
        }

        public ConditionRequest asCondition() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isMaterialiser() {
            return false;
        }

        public MaterialiserRequest asMaterialiser() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        protected static class ConditionRequest extends Request<Rule.Condition, ConceptMap, ConditionController> {

            public ConditionRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputPortId,
                                    Rule.Condition controllerId, ConceptMap processorId) {
                super(inputPortId, controllerId, processorId);
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

        protected static class MaterialiserRequest extends Request<Void, Materialisable, MaterialisationController> {

            public MaterialiserRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputPortId,
                                       Void controllerId, Materialisable processorId) {
                super(inputPortId, controllerId, processorId);
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
            extends AbstractProcessor<Either<ConceptMap, Materialisation>, OUTPUT, Request<?, ?, ?>, PROCESSOR> {

        private final Rule rule;
        private final ConceptMap bounds;
        private final ConceptManager conceptManager;
        private final Set<Identifier> conditionRequests;
        private final Set<Identifier> materialisationRequests;

        protected Processor(Driver<PROCESSOR> driver,
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

        protected Rule rule() {
            return rule;
        }

        @Override
        public void setUp() {
            setHubReactive(PoolingStream.fanOut(this));
            InputPort<Either<ConceptMap, Materialisation>> conditionInput = createInputPort();
            ConceptMap filteredBounds = bounds.filter(rule.condition().conjunction().retrieves());
            mayRequestCondition(new ConditionRequest(conditionInput.identifier(), rule.condition(), filteredBounds));
            Stream<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> conclusionReactive = fanIn(this, createOperator());
            conditionInput.map(Processor::convertConclusionInput).registerSubscriber(conclusionReactive);
            conclusionReactive.registerSubscriber(outputRouter());
        }

        protected abstract ConclusionOperator<OUTPUT> createOperator();

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
            // TODO: Does this method achieve anything?
            if (!materialisationRequests.contains(materialisationRequest.id())) {
                materialisationRequests.add(materialisationRequest.id());
                requestConnection(materialisationRequest);
            }
        }

        public static class Match extends Processor<Map<Variable, Concept>, Match> {

            protected Match(Driver<Match> driver, Driver<ConclusionController.Match> controller, Context context,
                            Rule rule, ConceptMap bounds, ConceptManager conceptManager, Supplier<String> debugName) {
                super(driver, controller, context, rule, bounds, conceptManager, debugName);
            }

            @Override
            protected ConclusionOperator<Map<Variable, Concept>> createOperator() {
                return new ConclusionOperator.Match(this);
            }
        }

        public static class Explain extends Processor<PartialExplanation, Explain> {

            protected Explain(Driver<Explain> driver,
                              Driver<? extends ConclusionController<PartialExplanation, Explain, ?>> controller,
                              Context context, Rule rule, ConceptMap bounds, ConceptManager conceptManager,
                              Supplier<String> debugName) {
                super(driver, controller, context, rule, bounds, conceptManager, debugName);
            }

            @Override
            protected ConclusionOperator<PartialExplanation> createOperator() {
                return new ConclusionOperator.Explain(this);
            }

        }

        private static abstract class ConclusionOperator<OUTPUT> implements Operator.Transformer<Either<ConceptMap, Map<Variable, Concept>>, OUTPUT> {

            private final Processor<?, ?> processor;

            private ConclusionOperator(Processor<?, ?> processor) {
                this.processor = processor;
            }

            protected Processor<?, ?> processor() {
                return processor;
            }

            @Override
            public Set<Publisher<Either<ConceptMap, Map<Variable, Concept>>>> initialNewPublishers() {
                return set();  // TODO: This could create the connection to the condition
            }

            @Override
            public Either<Publisher<Either<ConceptMap, Map<Variable, Concept>>>, Set<OUTPUT>> accept(
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher,
                    Either<ConceptMap, Map<Variable, Concept>> packet
            ) {
                if (packet.isFirst()) {
                    assert packet.first().concepts().keySet().containsAll(processor().rule().condition().conjunction().retrieves());
                    InputPort<Either<ConceptMap, Materialisation>> materialisationInput = processor().createInputPort();
                    ConceptMap filteredConditionAns = packet.first().filter(processor().rule().conclusion().retrievableIds());  // TODO: no explainables carried forwards
                    processor().mayRequestMaterialiser(new MaterialiserRequest(
                            materialisationInput.identifier(), null,
                            processor().rule().conclusion().materialisable(filteredConditionAns, processor().conceptManager))
                    );
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> op = materialisationInput
                            .map(m -> m.second().bindToConclusion(processor().rule().conclusion(), filteredConditionAns))
                            .flatMap(m -> merge(m, processor().bounds))
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

            public static class Match extends ConclusionOperator<Map<Variable, Concept>> {

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

            public static class Explain extends ConclusionOperator<PartialExplanation> {

                private final Map<Publisher<Either<ConceptMap, Map<Variable, Concept>>>, ConceptMap> conditionAnswers;

                private Explain(Processor<?, ?> processor) {
                    super(processor);
                    this.conditionAnswers = new HashMap<>();
                }

                @Override
                protected void mayStoreConditionAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialisationInput, ConceptMap conditionAnswer) {
                    conditionAnswers.put(materialisationInput, conditionAnswer);
                }

                @Override
                protected PartialExplanation packageAnswer(Publisher<Either<ConceptMap, Map<Variable, Concept>>> materialiserInput, Map<Variable, Concept> conclusionAnswer) {
                    return new PartialExplanation(processor().rule(), conclusionAnswer, conditionAnswers.get(materialiserInput));
                }
            }

        }

    }

}
