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

import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.reactive.RootSink;
import com.vaticle.typedb.core.reasoner.reactive.TransformationStream;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class RootConjunctionController
        extends ConjunctionController<ConceptMap, RootConjunctionController, RootConjunctionController.ReactiveBlock> {

    private final Set<Identifier.Variable.Retrievable> filter;
    private final boolean explain;
    private final Driver<Monitor> monitor;
    private final ReasonerConsumer<ConceptMap> reasonerConsumer;

    public RootConjunctionController(Driver<RootConjunctionController> driver, Conjunction conjunction,
                                     Set<Identifier.Variable.Retrievable> filter, boolean explain, ActorExecutorGroup executorService,
                                     Driver<Monitor> monitor, Registry registry,
                                     ReasonerConsumer<ConceptMap> reasonerConsumer) {
        super(driver, conjunction, executorService, registry);
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
                reactiveBlockDriver, driver(), monitor, bounds, plan(), filter, explain, reasonerConsumer,
                () -> ReactiveBlock.class.getSimpleName() + "(pattern:" + conjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(registry().conceptManager(), registry().logicManager()).hasNext())
                .toSet();
    }

    @Override
    public void exception(Throwable e) {
        super.exception(e);
        reasonerConsumer.exception(e);
    }

    protected static class ReactiveBlock extends ConjunctionController.ReactiveBlock<ConceptMap, ReactiveBlock> {

        private final Set<Identifier.Variable.Retrievable> filter;
        private RootSink<ConceptMap> rootSink;
        private final boolean explain;
        private final ReasonerConsumer<ConceptMap> reasonerConsumer;

        protected ReactiveBlock(Driver<ReactiveBlock> driver,
                                Driver<RootConjunctionController> controller, Driver<Monitor> monitor,
                                ConceptMap bounds, List<Resolvable<?>> plan,
                                Set<Identifier.Variable.Retrievable> filter,
                                boolean explain, ReasonerConsumer<ConceptMap> reasonerConsumer, Supplier<String> debugName) {
            super(driver, controller, monitor, bounds, plan, debugName);
            this.filter = filter;
            this.explain = explain;
            this.reasonerConsumer = reasonerConsumer;
        }

        @Override
        public void setUp() {
            Stream<ConceptMap, ConceptMap> op = TransformationStream.fanIn(
                    this, new CompoundOperator(this, plan, bounds)
            ).buffer();
            if (!explain) op = op.map(conceptMap -> conceptMap.filter(filter));
            op = op.distinct();
            setOutputRouter(op);
            rootSink = new RootSink<>(this, reasonerConsumer);
            outputRouter().registerSubscriber(rootSink);
        }

        @Override
        public void rootPull() {
            rootSink.pull();
        }

        @Override
        public void onFinished(Reactive.Identifier<?, ?> finishable) {
            assert !done;
//            done = true;
            assert finishable == rootSink.identifier();
            rootSink.finished();
        }
    }
}
