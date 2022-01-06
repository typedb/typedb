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

import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.controller.ConclusionPacket.ConditionBounds;
import com.vaticle.typedb.core.reasoner.controller.ConclusionPacket.MaterialisationBounds;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.function.Function;

import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class ConclusionController extends Controller<Rule.Condition, ConclusionPacket, VarConceptMap, ConclusionController.ConclusionProcessor, ConclusionController> {
    private final Rule.Conclusion conclusion;
    private final ControllerRegistry registry;

    protected ConclusionController(Driver<ConclusionController> driver, String name, Rule.Conclusion conclusion,
                                   ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService);
        this.conclusion = conclusion;
        this.registry = registry;
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc(VarConceptMap bounds) {
        return driver -> new ConclusionProcessor(
                driver, driver(), this.conclusion.rule(), bounds,
                ConclusionProcessor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
        );
    }

    @Override
    protected ConnectionBuilder<Rule.Condition, ConclusionPacket, ?, ?> getProviderController(ConnectionRequest<Rule.Condition, ConclusionPacket, ?> connectionRequest) {
        Driver<ConditionController> r = registry.registerConditionController(connectionRequest.pubControllerId());
        ConnectionBuilder<Rule.Condition, ConclusionPacket, ?, ?> c = connectionRequest.createConnectionBuilder(r);
        return c;
//        return connectionRequest.createConnectionBuilder(registry.registerConditionController(connectionRequest.pubControllerId()));
    }

    protected static class ConclusionProcessor extends Processor<ConclusionPacket, VarConceptMap, Rule.Condition, ConclusionProcessor> {

        private final Rule rule;
        private final VarConceptMap bounds;

        protected ConclusionProcessor(
                Driver<ConclusionProcessor> driver,
                Driver<? extends Controller<Rule.Condition,ConclusionPacket, VarConceptMap, ConclusionProcessor, ?>> controller,
                Rule rule, VarConceptMap bounds, String name) {
            super(driver, controller, name, noOp());
            this.rule = rule;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            requestConnection(driver(), rule.condition(), new ConditionBounds(bounds)).forEach(ans -> {
                requestConnection(driver(), null, new MaterialisationBounds(ans.asConditionAnswer().conceptMap(), rule.conclusion()))
                        .map(m -> m.asMaterialisationAnswer().concepts()).publishTo(outlet());
            });
        }
    }
}
