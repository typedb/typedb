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
import com.vaticle.typedb.core.reasoner.ReasonerProducer.EntryPoint;

import java.util.function.Function;

public class RootDisjunctionController
        extends DisjunctionController<RootDisjunctionController.RootDisjunctionProcessor, RootDisjunctionController> {
    private final EntryPoint reasonerEndpoint;

    public RootDisjunctionController(Driver<RootDisjunctionController> driver, Disjunction disjunction,
                                     ActorExecutorGroup executorService, Registry registry,
                                     EntryPoint reasonerEndpoint) {
        super(driver, disjunction, executorService, registry);
        this.reasonerEndpoint = reasonerEndpoint;
    }

    @Override
    protected Function<Driver<RootDisjunctionProcessor>, RootDisjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new RootDisjunctionProcessor(driver, driver(), disjunction, bounds, reasonerEndpoint,
                                                      RootDisjunctionProcessor.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")");
    }

    @Override
    public RootDisjunctionController asController() {
        return this;
    }

    @Override
    protected void exception(Throwable e) {
        super.exception(e);
        reasonerEndpoint.exception(e);
    }

    protected static class RootDisjunctionProcessor
            extends DisjunctionController.DisjunctionProcessor<RootDisjunctionController, RootDisjunctionProcessor> {

        private final EntryPoint reasonerEndpoint;

        protected RootDisjunctionProcessor(Driver<RootDisjunctionProcessor> driver,
                                           Driver<RootDisjunctionController> controller, Disjunction disjunction,
                                           ConceptMap bounds, EntryPoint reasonerEndpoint, String name) {
            super(driver, controller, disjunction, bounds, name);
            this.reasonerEndpoint = reasonerEndpoint;
            this.reasonerEndpoint.setMonitor(this);
        }

        @Override
        public void setUp() {
            super.setUp();
            outlet().publishTo(reasonerEndpoint);
        }

        @Override
        protected boolean isPulling() {
            return reasonerEndpoint.isPulling();
        }

        @Override
        protected boolean isMonitor() {
            return true;
        }

        @Override
        protected void onDone() {
            reasonerEndpoint.done();
        }
    }
}
