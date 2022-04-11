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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.computation.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveBlock;
import com.vaticle.typedb.core.reasoner.computation.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveBlock.Connector.Request;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.reasoner.computation.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.utils.Traversal;

import java.util.function.Supplier;

public class RetrievableController extends Controller<ConceptMap, Void, ConceptMap,
        Request<?, ?, Void>, RetrievableController.RetrievableReactiveBlock, RetrievableController> {

    private final Retrievable retrievable;
    private final Driver<Monitor> monitor;
    private final Registry registry;

    public RetrievableController(Driver<RetrievableController> driver, Retrievable retrievable,
                                 ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry) {
        super(driver, executorService, registry,
              () -> RetrievableController.class.getSimpleName() + "(pattern: " + retrievable + ")");
        this.retrievable = retrievable;
        this.monitor = monitor;
        this.registry = registry;
    }

    @Override
    public void setUpUpstreamControllers() {
        // None to set up
    }

    @Override
    protected RetrievableReactiveBlock createReactiveBlockFromDriver(Driver<RetrievableReactiveBlock> reactiveBlockDriver, ConceptMap conceptMap) {
        return new RetrievableReactiveBlock(
                reactiveBlockDriver, driver(), monitor, () -> Traversal.traversalIterator(registry, retrievable.pattern(), conceptMap),
                () -> RetrievableReactiveBlock.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ", bounds: " + conceptMap.toString() + ")"
        );
    }

    @Override
    public void resolveController(Request<?, ?, Void> connectionRequest) {
        // Nothing to do
    }

    protected static class RetrievableReactiveBlock extends ReactiveBlock<Void, ConceptMap,
                Request<?, ?, Void>, RetrievableReactiveBlock> {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        protected RetrievableReactiveBlock(Driver<RetrievableReactiveBlock> driver, Driver<RetrievableController> controller,
                                       Driver<Monitor> monitor,
                                       Supplier<FunctionalIterator<ConceptMap>> traversalSupplier,
                                       Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.traversalSupplier = traversalSupplier;
        }

        @Override
        public void setUp() {
            setOutputRouter(PoolingStream.fanOut(this));
            Source.create(this, new Operator.Supplier<>(traversalSupplier)).registerSubscriber(outputRouter());
        }
    }
}
