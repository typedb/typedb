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
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanOutStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.SingleReceiverStream;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class ConclusionController extends Controller<ConceptMap, Either<ConceptMap, Materialisation>, Map<Variable, Concept>,
        ConclusionController.ConclusionProcessor, ConclusionController> {
    private final Rule.Conclusion conclusion;
    private final Driver<MaterialiserController> materialiserController;
    private final Monitor.MonitorRef monitorRef;
    private Driver<ConditionController> conditionController;

    public ConclusionController(Driver<ConclusionController> driver, String name, Rule.Conclusion conclusion,
                                ActorExecutorGroup executorService, Driver<MaterialiserController> materialiserController,
                                Monitor.MonitorRef monitorRef, Registry registry) {
        super(driver, executorService, registry, name);
        this.conclusion = conclusion;
        this.materialiserController = materialiserController;
        this.monitorRef = monitorRef;
    }

    @Override
    public void setUpUpstreamProviders() {
        conditionController = registry().registerConditionController(conclusion.rule().condition());
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new ConclusionProcessor(
                driver, driver(), monitorRef, this.conclusion.rule(), bounds, registry().conceptManager(),
                ConclusionProcessor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public ConclusionController asController() {
        return this;
    }

    private Driver<MaterialiserController> materialiserController() {
        return materialiserController;
    }

    private Driver<ConditionController> conditionController() {
        return conditionController;
    }

    protected static class ConclusionProcessor extends Processor<Either<ConceptMap, Materialisation>, Map<Variable, Concept>, ConclusionController, ConclusionProcessor> {

        private final Rule rule;
        private final ConceptMap bounds;
        private final ConceptManager conceptManager;
        private final Set<ConditionRequest> conditionRequests;
        private final Set<MaterialiserRequest> materialiserRequests;

        protected ConclusionProcessor(Driver<ConclusionProcessor> driver,
                                      Driver<ConclusionController> controller, Monitor.MonitorRef monitorRef, Rule rule,
                                      ConceptMap bounds, ConceptManager conceptManager, String name) {
            super(driver, controller, monitorRef, name);
            this.rule = rule;
            this.bounds = bounds;
            this.conceptManager = conceptManager;
            this.conditionRequests = new HashSet<>();
            this.materialiserRequests = new HashSet<>();
        }

        @Override
        public void setUp() {
            setOutlet(new FanOutStream<>(monitor(), name()));
            InletEndpoint<Either<ConceptMap, Materialisation>> conditionEndpoint = createReceivingEndpoint();
            mayRequestCondition(new ConditionRequest(driver(), conditionEndpoint.id(), rule.condition(), bounds));
            ConclusionReactive conclusionReactive = new ConclusionReactive(name(), monitor());
            conditionEndpoint.map(a -> a.first()).publishTo(conclusionReactive);
            conclusionReactive.publishTo(outlet());
        }

        private void mayRequestCondition(ConditionRequest conditionRequest) {
            if (!conditionRequests.contains(conditionRequest)) {
                conditionRequests.add(conditionRequest);
                requestConnection(conditionRequest);
            }
        }

        private void mayRequestMaterialiser(MaterialiserRequest materialiserRequest) {
            if (!materialiserRequests.contains(materialiserRequest)) {
                materialiserRequests.add(materialiserRequest);
                requestConnection(materialiserRequest);
            }
        }

        protected static class ConditionRequest extends Request<Rule.Condition, ConceptMap, ConditionController, Either<ConceptMap, Materialisation>, ConclusionProcessor, ConclusionController, ConditionRequest> {

            public ConditionRequest(Driver<ConclusionProcessor> recProcessor, long recEndpointId,
                                    Rule.Condition provControllerId, ConceptMap provProcessorId) {
                super(recProcessor, recEndpointId, provControllerId, provProcessorId);
            }

            @Override
            public Builder<ConceptMap, Either<ConceptMap, Materialisation>, ConditionRequest, ConclusionProcessor, ?> getBuilder(ConclusionController controller) {
                return new Builder<>(controller.conditionController(), this);
            }

        }

        protected static class MaterialiserRequest extends Request<Void, Materialisable, MaterialiserController, Either<ConceptMap, Materialisation>, ConclusionProcessor, ConclusionController, MaterialiserRequest> {

            public MaterialiserRequest(Driver<ConclusionProcessor> recProcessor, long recEndpointId,
                                       Void provControllerId, Materialisable provProcessorId) {
                super(recProcessor, recEndpointId, provControllerId, provProcessorId);
            }

            @Override
            public Builder<Materialisable, Either<ConceptMap, Materialisation>, MaterialiserRequest, ConclusionProcessor, ?> getBuilder(ConclusionController controller) {
                return new Builder<>(controller.materialiserController(), this);
            }

        }

        private class ConclusionReactive extends SingleReceiverStream<ConceptMap, Map<Variable, Concept>> {

            private final ProviderRegistry.SingleProviderRegistry<ConceptMap> providerRegistry;
            private ProviderRegistry.MultiProviderRegistry<Map<Variable, Concept>> materialiserRegistry;

            protected ConclusionReactive(String groupName, Monitor.MonitorRef monitor) {
                super(monitor, groupName);
                this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(this, monitor);
                this.materialiserRegistry = null;
            }

            @Override
            public void publishTo(Subscriber<Map<Variable, Concept>> subscriber) {
                super.publishTo(subscriber);
                // We need to wait until the receiver has been given before we can create the materialiser registry
                this.materialiserRegistry = new ProviderRegistry.MultiProviderRegistry<>(receiverRegistry().receiver(), monitor());
            }

            @Override
            protected ProviderRegistry<ConceptMap> providerRegistry() {
                return providerRegistry;
            }

            protected ProviderRegistry.MultiProviderRegistry<Map<Variable, Concept>> materialiserRegistry() {
                return materialiserRegistry;
            }

            @Override
            public void pull(Receiver<Map<Variable, Concept>> receiver) {
                super.pull(receiver);
                materialiserRegistry().pullAll();
            }

            @Override
            public void receive(Provider<ConceptMap> provider, ConceptMap packet) {
                super.receive(provider, packet);
                InletEndpoint<Either<ConceptMap, Materialisation>> materialiserEndpoint = createReceivingEndpoint();
                mayRequestMaterialiser(new MaterialiserRequest(
                        driver(), materialiserEndpoint.id(), null,
                        rule.conclusion().materialisable(packet, conceptManager))
                );
                Stream<?, Map<Variable, Concept>> op = materialiserEndpoint.map(m -> m.second().bindToConclusion(rule.conclusion(), packet));
                MaterialiserReactive materialiserReactive = new MaterialiserReactive(this, monitor(), groupName());
                materialiserRegistry().add(materialiserReactive);
                op.publishTo(materialiserReactive);
                materialiserReactive.sendTo(receiverRegistry().receiver());

                monitor().syncAndReportPathFork(1, this);
                monitor().consumeAnswer(this);

                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, materialiserReactive));
                materialiserRegistry().pull(materialiserReactive);
                providerRegistry().pull(provider);  // We need to pull on the condition again in case materialisation fails
            }

            private void receiveMaterialisation(MaterialiserReactive provider, Map<Variable, Concept> packet) {
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
                receiverRegistry().setNotPulling();
                receiverRegistry().receiver().receive(this, packet);
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, provider));
                provider.pull(receiverRegistry().receiver());  // We need to pull again so that the materialiser processor does a join of its own accord
            }
        }

        private class MaterialiserReactive extends SingleReceiverStream<Map<Variable, Concept>, Map<Variable, Concept>> {

            private final ConclusionReactive parent;
            private final ProviderRegistry.SingleProviderRegistry<Map<Variable, Concept>> providerRegistry;

            public MaterialiserReactive(ConclusionReactive parent, Monitor.MonitorRef monitor, String groupName) {
                super(monitor, groupName);
                this.parent = parent;
                this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(this, monitor);
            }

            @Override
            protected ProviderRegistry<Map<Variable, Concept>> providerRegistry() {
                return providerRegistry;
            }

            @Override
            public void receive(Provider<Map<Variable, Concept>> provider, Map<Variable, Concept> packet) {
                super.receive(provider, packet);
                receiverRegistry().setNotPulling();
                parent.receiveMaterialisation(this, packet);
            }

        }

    }

}
