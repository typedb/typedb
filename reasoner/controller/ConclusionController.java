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
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisable;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisation;
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanOutStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.SingleReceiverSingleProviderStream;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class ConclusionController extends Controller<ConceptMap, Either<ConceptMap, Materialisation>, Map<Variable, Concept>,
        ConclusionController.ConclusionProcessor, ConclusionController> {
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
    public void setUpUpstreamProviders() {
        conditionController = registry().registerConditionController(conclusion.rule().condition());
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new ConclusionProcessor(
                driver, driver(), monitor, this.conclusion.rule(), bounds, registry().conceptManager(),
                () -> ConclusionProcessor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public ConclusionController getThis() {
        return this;
    }

    private Driver<MaterialisationController> materialisationController() {
        return materialisationController;
    }

    private Driver<ConditionController> conditionController() {
        return conditionController;
    }

    protected static class ConditionRequest extends ProviderRequest<Rule.Condition, ConceptMap, Either<ConceptMap, Materialisation>, ConclusionController> {

        public ConditionRequest(Reactive.Identifier.Input<Either<ConceptMap, Materialisation>> recEndpointId,
                                Rule.Condition provControllerId, ConceptMap provProcessorId) {
            super(recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, Either<ConceptMap, Materialisation>> getConnectionBuilder(ConclusionController controller) {
            return new Connection.Builder<>(controller.conditionController(), this);
        }

    }

    protected static class MaterialiserRequest extends ProviderRequest<Void, Materialisable, Either<ConceptMap, Materialisation>, ConclusionController> {

        public MaterialiserRequest(Reactive.Identifier.Input<Either<ConceptMap, Materialisation>> recEndpointId,
                                   Void provControllerId, Materialisable provProcessorId) {
            super(recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<Materialisable, Either<ConceptMap, Materialisation>> getConnectionBuilder(ConclusionController controller) {
            return new Connection.Builder<>(controller.materialisationController(), this);
        }

    }

    protected static class ConclusionProcessor extends Processor<Either<ConceptMap, Materialisation>, Map<Variable, Concept>, ConclusionController, ConclusionProcessor> {

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
            setOutlet(new FanOutStream<>(this));
            InletEndpoint<Either<ConceptMap, Materialisation>> conditionEndpoint = createReceivingEndpoint();
            mayRequestCondition(new ConditionRequest(conditionEndpoint.identifier(), rule.condition(), bounds));
            ConclusionReactive conclusionReactive = new ConclusionReactive(this);
            conditionEndpoint.map(a -> a.first()).publishTo(conclusionReactive);
            conclusionReactive.publishTo(outlet());
        }

        private void mayRequestCondition(ConditionRequest conditionRequest) {
            // TODO: Does this methods achieve anything?
            if (!conditionRequests.contains(conditionRequest)) {
                conditionRequests.add(conditionRequest);
                requestProvider(conditionRequest);
            }
        }

        private void mayRequestMaterialiser(MaterialiserRequest materialisationRequest) {
            // TODO: Does this methods achieve anything?
            if (!materialisationRequests.contains(materialisationRequest)) {
                materialisationRequests.add(materialisationRequest);
                requestProvider(materialisationRequest);
            }
        }

        private class ConclusionReactive extends SingleReceiverSingleProviderStream<ConceptMap, Map<Variable, Concept>> {

            private ProviderRegistry.MultiProviderRegistry<Provider.Sync<Map<Variable, Concept>>> materialisationRegistry;

            protected ConclusionReactive(Processor<?, ?, ?, ?> processor) {
                super(processor);
                this.materialisationRegistry = null;
            }

            @Override
            public void publishTo(Subscriber<Map<Variable, Concept>> subscriber) {
                super.publishTo(subscriber);
                // We need to wait until the receiver has been given before we can create the materialisation registry
                this.materialisationRegistry = new ProviderRegistry.MultiProviderRegistry<>(receiverRegistry().receiver(), processor());
            }

            protected ProviderRegistry.MultiProviderRegistry<Provider.Sync<Map<Variable, Concept>>> materialisationRegistry() {
                return materialisationRegistry;
            }

            @Override
            public void pull(Receiver.Sync<Map<Variable, Concept>> receiver) {
                super.pull(receiver);
                materialisationRegistry().nonPulling().forEach(p -> p.pull(receiverRegistry().receiver()));
            }

            @Override
            public void receive(Provider.Sync<ConceptMap> provider, ConceptMap packet) {
                super.receive(provider, packet);
                InletEndpoint<Either<ConceptMap, Materialisation>> materialisationEndpoint = createReceivingEndpoint();
                mayRequestMaterialiser(new MaterialiserRequest(
                        materialisationEndpoint.identifier(), null,
                        rule.conclusion().materialisable(packet, conceptManager))
                );
                Stream<?, Map<Variable, Concept>> op = materialisationEndpoint.map(m -> m.second().bindToConclusion(rule.conclusion(), packet));
                MaterialisationReactive materialisationReactive = new MaterialisationReactive(this, processor());
                materialisationRegistry().add(materialisationReactive);
                op.publishTo(materialisationReactive);
                materialisationReactive.sendTo(receiverRegistry().receiver());

                processor().monitor().execute(actor -> actor.forkFrontier(1, identifier()));
                processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));

                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, materialisationReactive));
                if (receiverRegistry().isPulling()) {
                    if (!materialisationRegistry().isPulling(materialisationReactive)) materialisationReactive.pull(receiverRegistry().receiver());
                    processor().driver().execute(actor -> actor.retryPull(provider.identifier(), identifier()));  // We need to retry the condition again in case materialisation fails
                }
            }

            private void receiveMaterialisation(MaterialisationReactive provider, Map<Variable, Concept> packet) {
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
                materialisationRegistry().recordReceive(provider);
                if (receiverRegistry().isPulling()) {
                    Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, provider));
                    processor().driver().execute(actor -> actor
                            .retryPull(provider.identifier(), receiverRegistry().receiver().identifier()));  // We need to retry so that the materialisation does a join
                }
                receiverRegistry().setNotPulling();
                receiverRegistry().receiver().receive(this, packet);
            }
        }

        private class MaterialisationReactive extends SingleReceiverSingleProviderStream<Map<Variable, Concept>, Map<Variable, Concept>> {

            private final ConclusionReactive parent;

            public MaterialisationReactive(ConclusionReactive parent, Processor<?, ?, ?, ?> processor) {
                super(processor);
                this.parent = parent;
            }

            @Override
            public void receive(Provider.Sync<Map<Variable, Concept>> provider, Map<Variable, Concept> packet) {
                super.receive(provider, packet);
                receiverRegistry().setNotPulling();
                parent.receiveMaterialisation(this, packet);
            }

        }

    }

}
