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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.function.Function;

import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class ConclusionController extends Controller<ConceptMap, ConclusionPacket, VarConceptMap,
        ConclusionController.ConclusionRequest, ConclusionController.ConclusionProcessor, ConclusionController> {
    private final Rule.Conclusion conclusion;

    protected ConclusionController(Driver<ConclusionController> driver, String name, Rule.Conclusion conclusion,
                                   ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService, registry);
        this.conclusion = conclusion;
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new ConclusionProcessor(
                driver, driver(), this.conclusion.rule(), bounds,
                ConclusionProcessor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
        );
    }

    protected static class ConclusionRequest extends ConnectionRequest<Rule.Condition, ConceptMap, ConclusionPacket, ConclusionController.ConclusionProcessor> {

        public ConclusionRequest(Driver<ConclusionProcessor> recProcessor, long recEndpointId,
                                 Rule.Condition provControllerId, ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public ConnectionBuilder<ConceptMap, ConclusionPacket, ConnectionRequest<Rule.Condition, ConceptMap,
                ConclusionPacket, ConclusionProcessor>, ConclusionProcessor, ?> getBuilder(ControllerRegistry registry) {
            return createConnectionBuilder(registry.registerConditionController(pubControllerId()));
        }
    }

    protected static class ConclusionProcessor extends Processor<ConclusionPacket, VarConceptMap, ConclusionRequest, ConclusionProcessor> {

        private final Rule rule;
        private final ConceptMap bounds;

        protected ConclusionProcessor(
                Driver<ConclusionProcessor> driver,
                Driver<? extends Controller<?, ?, ?, ConclusionRequest, ConclusionProcessor, ?>> controller, Rule rule,
                ConceptMap bounds, String name
        ) {
            super(driver, controller, name, noOp());
            this.rule = rule;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            InletEndpoint<ConclusionPacket> conditionEndpoint = createReceivingEndpoint();
            requestConnection(new ConclusionRequest(driver(), conditionEndpoint.id(), rule.condition(), bounds));
            conditionEndpoint.forEach(ans -> {
                InletEndpoint<ConclusionPacket> materialiserEndpoint = createReceivingEndpoint();
                requestConnection(new ConclusionRequest(driver(), materialiserEndpoint.id(), null, ans.asConditionAnswer().conceptMap()));
                    materialiserEndpoint.map(m -> m.asMaterialisationAnswer().concepts()).publishTo(outlet());
            });
        }
    }
}
