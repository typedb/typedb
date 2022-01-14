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
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.function.Function;

public class RootDisjunctionController extends DisjunctionController<RootDisjunctionController, RootDisjunctionController.RootDisjunctionProcessor> {
    private final Receiver.Subscriber<ConceptMap> reasonerEndpoint;

    public RootDisjunctionController(Driver<RootDisjunctionController> driver, Disjunction disjunction,
                                     ActorExecutorGroup executorService, ControllerRegistry registry,
                                     Receiver.Subscriber<ConceptMap> reasonerEndpoint) {
        super(driver, disjunction, executorService, registry);
        this.reasonerEndpoint = reasonerEndpoint;
    }

    @Override
    protected Function<Driver<RootDisjunctionProcessor>, RootDisjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new RootDisjunctionProcessor(driver, driver(), disjunction, bounds, reasonerEndpoint,
                                                      RootDisjunctionProcessor.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")");
    }

    protected static class RootDisjunctionProcessor extends DisjunctionController.DisjunctionProcessor<RootDisjunctionProcessor> {

        private final Receiver.Subscriber<ConceptMap> reasonerEndpoint;

        protected RootDisjunctionProcessor(Driver<RootDisjunctionProcessor> driver,
                                           Driver<? extends Controller<?, ConceptMap, ?, RootDisjunctionProcessor, ?>> controller,
                                           Disjunction disjunction,
                                           ConceptMap bounds, Receiver.Subscriber<ConceptMap> reasonerEndpoint, String name) {
            super(driver, controller, disjunction, bounds, name);
            this.reasonerEndpoint = reasonerEndpoint;
        }

        @Override
        public void setUp() {
            outlet().publishTo(reasonerEndpoint);
            super.setUp();
        }
    }
}
