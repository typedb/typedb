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
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisable;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisation;
import com.vaticle.typedb.core.reasoner.computation.actor.Connector;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.Input;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.TransformationStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanOutStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.SingleReceiverMultiProviderStream;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController.FromConclusionRequest.ConditionRequest;
import com.vaticle.typedb.core.reasoner.controller.ConclusionController.FromConclusionRequest.MaterialiserRequest;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ConclusionController extends Controller<ConceptMap, Either<ConceptMap, Materialisation>, Map<Variable, Concept>,
        ConclusionController.FromConclusionRequest<?, ?>, ConclusionController.ConclusionProcessor, ConclusionController> {
    private final Rule.Conclusion conclusion;
    private final Driver<MaterialisationController> materialisationController;
    private final Driver<Monitor> monitor;
    private Driver<ConditionController> conditionController;

    public ConclusionController(Driver<ConclusionController> driver, Rule.Conclusion conclusion,
                                ActorExecutorGroup executorService, Driver<MaterialisationController> materialisationController,
                                Driver<Monitor> monitor, Registry registry) {
        super(driver, executorService, registry, () -> ConclusionController.class.getSimpleName() + "(pattern: " + conclusion + ")");
        this.conclusion = conclusion;
        this.materialisationController = materialisationController;
        this.monitor = monitor;
    }

    @Override
    public void setUpUpstreamControllers() {
        conditionController = registry().registerConditionController(conclusion.rule().condition());
    }

    @Override
    protected ConclusionProcessor createProcessorFromDriver(Driver<ConclusionProcessor> processorDriver, ConceptMap bounds) {
        return new ConclusionProcessor(
                processorDriver, driver(), monitor, this.conclusion.rule(), bounds, registry().conceptManager(),
                () -> ConclusionProcessor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
        );
    }

    @Override
    protected void resolveController(FromConclusionRequest<?, ?> req) {
        if (isTerminated()) return;
        if (req.isCondition()) {
            conditionController.execute(actor -> actor.resolveProcessor(
                    new Connector<>(req.asCondition().inputId(), req.asCondition().bounds())));
        } else if (req.isMaterialiser()) {
            materialisationController.execute(actor -> actor.resolveProcessor(
                    new Connector<>(req.asMaterialiser().inputId(), req.asMaterialiser().bounds())));
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    protected static class FromConclusionRequest<CONTROLLER_ID, BOUNDS> extends Connector.Request<CONTROLLER_ID, BOUNDS, Either<ConceptMap, Materialisation>> {

        protected FromConclusionRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputId,
                                        CONTROLLER_ID controller_id, BOUNDS bounds) {
            super(inputId, controller_id, bounds);
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

        protected static class ConditionRequest extends FromConclusionRequest<Rule.Condition, ConceptMap> {

            public ConditionRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputId,
                                    Rule.Condition controllerId, ConceptMap processorId) {
                super(inputId, controllerId, processorId);
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

        protected static class MaterialiserRequest extends FromConclusionRequest<Void, Materialisable> {

            public MaterialiserRequest(Reactive.Identifier<Either<ConceptMap, Materialisation>, ?> inputId,
                                       Void controllerId, Materialisable processorId) {
                super(inputId, controllerId, processorId);
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

    protected static class ConclusionProcessor extends Processor<Either<ConceptMap, Materialisation>, Map<Variable, Concept>, FromConclusionRequest<?, ?>, ConclusionProcessor> {

        private final Rule rule;
        private final ConceptMap bounds;
        private final ConceptManager conceptManager;
        private final Set<ConditionRequest> conditionRequests;
        private final Set<MaterialiserRequest> materialisationRequests;

        protected ConclusionProcessor(Driver<ConclusionProcessor> driver,
                                      Driver<ConclusionController> controller, Driver<Monitor> monitor, Rule rule,
                                      ConceptMap bounds, ConceptManager conceptManager, Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.rule = rule;
            this.bounds = bounds;
            this.conceptManager = conceptManager;
            this.conditionRequests = new HashSet<>();
            this.materialisationRequests = new HashSet<>();
        }

        @Override
        public void setUp() {
            setOutputRouter(new FanOutStream<>(this));
            Input<Either<ConceptMap, Materialisation>> conditionInput = createInput();
            mayRequestCondition(new ConditionRequest(conditionInput.identifier(), rule.condition(), bounds));
            Stream<Either<ConceptMap, Map<Variable, Concept>>, Map<Variable, Concept>> conclusionReactive =
                    TransformationStream.fanIn(this, new ConclusionOperator(this));
            conditionInput.map(ConclusionProcessor::convertConclusionInput).registerReceiver(conclusionReactive);
            conclusionReactive.registerReceiver(outputRouter());
        }

        private static Either<ConceptMap, Map<Variable, Concept>> convertConclusionInput(Either<ConceptMap, Materialisation> input) {
            return Either.first(input.first());
        }

        private void mayRequestCondition(ConditionRequest conditionRequest) {
            // TODO: Does this method achieve anything?
            if (!conditionRequests.contains(conditionRequest)) {
                conditionRequests.add(conditionRequest);
                requestConnection(conditionRequest);
            }
        }

        private void mayRequestMaterialiser(MaterialiserRequest materialisationRequest) {
            // TODO: Does this method achieve anything?
            if (!materialisationRequests.contains(materialisationRequest)) {
                materialisationRequests.add(materialisationRequest);
                requestConnection(materialisationRequest);
            }
        }

        public static class ConclusionOperator implements Operator.Transformer<Either<ConceptMap, Map<Variable, Concept>>, Map<Variable, Concept>>, Operator {

            private final ConclusionProcessor processor;

            ConclusionOperator(ConclusionController.ConclusionProcessor processor) {
                this.processor = processor;
            }

            private ConclusionProcessor processor() {
                return processor;
            }

            @Override
            public Transformed<Map<Variable, Concept>, Either<ConceptMap, Map<Variable, Concept>>> accept(Publisher<Either<ConceptMap, Map<Variable, Concept>>> publisher, Either<ConceptMap, Map<Variable, Concept>> packet) {
                Transformed<Map<Variable, Concept>, Either<ConceptMap, Map<Variable, Concept>>> outcome = Transformed.create();
                if (packet.isFirst()) {
                    Input<Either<ConceptMap, Materialisation>> materialisationInput = processor().createInput();
                    processor().mayRequestMaterialiser(new MaterialiserRequest(
                            materialisationInput.identifier(), null,
                            processor().rule.conclusion().materialisable(packet.first(), processor().conceptManager))
                    );
                    Publisher<Either<ConceptMap, Map<Variable, Concept>>> op = materialisationInput
                            .map(m -> Either.second(m.second().bindToConclusion(processor().rule.conclusion(), packet.first())));
                    outcome.addNewPublisher(op);
                    outcome.addAnswerConsumed();
                } else {
                    outcome.addOutput(packet.second());
                }
                return outcome;
            }
        }

    }

}
