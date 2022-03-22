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
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.EntryPoint;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanInStream;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class RootDisjunctionController
        extends DisjunctionController<RootDisjunctionController.RootDisjunctionProcessor, RootDisjunctionController> {
    private final Set<Identifier.Variable.Retrievable> filter;
    private final Driver<Monitor> monitor;
    private final ReasonerConsumer reasonerConsumer;

    public RootDisjunctionController(Driver<RootDisjunctionController> driver, Disjunction disjunction,
                                     Set<Identifier.Variable.Retrievable> filter, ActorExecutorGroup executorService,
                                     Driver<Monitor> monitor, Registry registry,
                                     ReasonerConsumer reasonerConsumer) {
        super(driver, disjunction, executorService, registry);
        this.filter = filter;
        this.monitor = monitor;
        this.reasonerConsumer = reasonerConsumer;
    }

    @Override
    protected Function<Driver<RootDisjunctionProcessor>, RootDisjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new RootDisjunctionProcessor(
                driver, driver(), monitor, disjunction, bounds, filter, reasonerConsumer,
                () -> RootDisjunctionProcessor.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public RootDisjunctionController getThis() {
        return this;
    }

    @Override
    protected void exception(Throwable e) {
        super.exception(e);
        reasonerConsumer.exception(e);
    }

    protected static class RootDisjunctionProcessor
            extends DisjunctionController.DisjunctionProcessor<RootDisjunctionController, RootDisjunctionProcessor> {

        private final Set<Identifier.Variable.Retrievable> filter;
        private final ReasonerConsumer reasonerConsumer;
        private EntryPoint reasonerEntryPoint;

        protected RootDisjunctionProcessor(Driver<RootDisjunctionProcessor> driver,
                                           Driver<RootDisjunctionController> controller, Driver<Monitor> monitor, Disjunction disjunction,
                                           ConceptMap bounds, Set<Identifier.Variable.Retrievable> filter,
                                           ReasonerConsumer reasonerConsumer, Supplier<String> debugName) {
            super(driver, controller, monitor, disjunction, bounds, debugName);
            this.filter = filter;
            this.reasonerConsumer = reasonerConsumer;
        }

        @Override
        public void setUp() {
            super.setUp();
            reasonerEntryPoint = new EntryPoint(this, reasonerConsumer);
            outputRouter().publishTo(reasonerEntryPoint);
        }

        @Override
        public void pull() {
            reasonerEntryPoint.pull();
        }

        @Override
        protected Reactive.Stream<ConceptMap, ConceptMap> getOutputRouter(FanInStream<ConceptMap> fanIn) {
            // Simply here to be overridden by root disjuntion to avoid duplicating setUp
            return fanIn.buffer().map(conceptMap -> conceptMap.filter(filter)).deduplicate();
        }

        @Override
        protected void onFinished(Reactive.Identifier finishable) {
            assert !done;
//            done = true;
            assert finishable == reasonerEntryPoint.identifier();
            reasonerEntryPoint.onFinished();
        }
    }
}
