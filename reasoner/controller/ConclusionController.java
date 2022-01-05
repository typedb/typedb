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

public class ConclusionController extends Controller<Rule.Condition, ConceptMap, ConclusionController.ConclusionProcessor, ConclusionController> {
    private final Rule.Conclusion conclusion;
    private final ControllerRegistry registry;

    protected ConclusionController(Driver<ConclusionController> driver, String name, Rule.Conclusion conclusion,
                                   ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService);
        this.conclusion = conclusion;
        this.registry = registry;
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new ConclusionProcessor(
                driver, driver(),
                ConclusionProcessor.class.getSimpleName() + "(pattern: " + conclusion + ", bounds: " + bounds + ")"
        );
    }

    @Override
    protected ConnectionBuilder<Rule.Condition, ConceptMap, ?, ?> getProviderController(ConnectionRequest<Rule.Condition, ConceptMap, ?> connectionRequest) {
        return connectionRequest.createConnectionBuilder(registry.registerConditionController(connectionRequest.pubControllerId()));
    }

    public static class ConclusionProcessor extends Processor<ConceptMap, Rule.Condition, ConclusionProcessor> {
        protected ConclusionProcessor(Driver<ConclusionProcessor> driver, Driver<? extends Controller<Rule.Condition,
                ConceptMap, ConclusionProcessor, ?>> controller, String name) {
            super(driver, controller, name, noOp());
        }

        @Override
        public void setUp() {

        }
    }
}
