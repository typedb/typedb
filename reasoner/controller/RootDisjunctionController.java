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
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.RootSink;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;
import java.util.function.Supplier;

public class RootDisjunctionController
        extends DisjunctionController<RootDisjunctionController.ReactiveBlock, RootDisjunctionController> {

    private final Set<Identifier.Variable.Retrievable> filter;
    private final boolean explain;
    private final Driver<Monitor> monitor;
    private final ReasonerConsumer<ConceptMap> reasonerConsumer;

    public RootDisjunctionController(Driver<RootDisjunctionController> driver, Disjunction disjunction,
                                     Set<Identifier.Variable.Retrievable> filter, boolean explain,
                                     ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry,
                                     ReasonerConsumer<ConceptMap> reasonerConsumer) {
        super(driver, disjunction, executorService, registry);
        this.filter = filter;
        this.explain = explain;
        this.monitor = monitor;
        this.reasonerConsumer = reasonerConsumer;
    }

    @Override
    public void initialise() {
        setUpUpstreamControllers();
        createReactiveBlockIfAbsent(new ConceptMap());
    }

    @Override
    protected ReactiveBlock createReactiveBlockFromDriver(Driver<ReactiveBlock> reactiveBlockDriver, ConceptMap bounds) {
        return new ReactiveBlock(
                reactiveBlockDriver, driver(), monitor, disjunction, bounds, filter, explain, reasonerConsumer,
                () -> ReactiveBlock.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        reasonerConsumer.exception(cause);
    }

    protected static class ReactiveBlock extends DisjunctionController.ReactiveBlock<ReactiveBlock> {

        private final Set<Identifier.Variable.Retrievable> filter;
        private final boolean explain;
        private final ReasonerConsumer<ConceptMap> reasonerConsumer;
        private RootSink<ConceptMap> rootSink;

        protected ReactiveBlock(Driver<ReactiveBlock> driver,
                                Driver<RootDisjunctionController> controller, Driver<Monitor> monitor,
                                Disjunction disjunction, ConceptMap bounds,
                                Set<Identifier.Variable.Retrievable> filter, boolean explain,
                                ReasonerConsumer<ConceptMap> reasonerConsumer, Supplier<String> debugName) {
            super(driver, controller, monitor, disjunction, bounds, debugName);
            this.filter = filter;
            this.explain = explain;
            this.reasonerConsumer = reasonerConsumer;
        }

        @Override
        public void setUp() {
            super.setUp();
            rootSink = new RootSink<>(this, reasonerConsumer);
            outputRouter().registerSubscriber(rootSink);
        }

        @Override
        public void rootPull() {
            rootSink.pull();
        }

        @Override
        protected Reactive.Stream<ConceptMap, ConceptMap> getOutputRouter(Reactive.Stream<ConceptMap, ConceptMap> fanIn) {
            // Simply here to be overridden by root disjuntion to avoid duplicating setUp
            Reactive.Stream<ConceptMap, ConceptMap> op = fanIn;
            if (!explain) op = op.map(conceptMap -> conceptMap.filter(filter));
            op = op.distinct();
            return op;
        }

        @Override
        public void onFinished(Reactive.Identifier<?, ?> finishable) {
            assert finishable == rootSink.identifier();
            rootSink.finished();
        }
    }
}
