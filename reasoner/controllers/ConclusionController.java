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

package com.vaticle.typedb.core.reasoner.controllers;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;

import java.util.function.Function;

public class ConclusionController extends Controller<Conjunction, ConceptMap, ConclusionController.ConclusionProcessor, ConclusionController> {
    protected ConclusionController(Driver<ConclusionController> driver, String name, Rule.Conclusion id, ActorExecutorGroup executorService) {
        super(driver, name, executorService);
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc(ConceptMap id) {
        return null;  // TODO
    }

    @Override
    protected ConnectionBuilder<Conjunction, ConceptMap, ?, ?> getPublisherController(ConnectionRequest<Conjunction, ConceptMap, ?> connectionRequest) {
        return null;  // TODO
    }

    public static class ConclusionProcessor extends Processor<ConceptMap, Conjunction, ConclusionProcessor> {
        protected ConclusionProcessor(Driver<ConclusionProcessor> driver, Driver<? extends Controller<Conjunction,
                ConceptMap, ConclusionProcessor, ?>> controller, String name, Reactive<ConceptMap, ConceptMap> outlet) {
            super(driver, controller, name, outlet);
        }

        @Override
        public void setUp() {

        }
    }
}
