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
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock.Connector.AbstractRequest;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.Source;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator;

import java.util.function.Supplier;

public class RetrievableController extends AbstractController<
        ConceptMap,
        Void,
        ConceptMap,
        AbstractRequest<?, ?, Void>,
        RetrievableController.RetrievableReactiveBlock,
        RetrievableController
        > {

    private final Retrievable retrievable;

    public RetrievableController(Driver<RetrievableController> driver, Retrievable retrievable,
                                 Context context) {
        super(driver, context, () -> RetrievableController.class.getSimpleName() + "(pattern: " + retrievable + ")");
        this.retrievable = retrievable;
    }

    @Override
    public void setUpUpstreamControllers() {
        // None to set up
    }

    @Override
    protected RetrievableReactiveBlock createReactiveBlockFromDriver(
            Driver<RetrievableReactiveBlock> reactiveBlockDriver, ConceptMap conceptMap
    ) {
        return new RetrievableReactiveBlock(
                reactiveBlockDriver, driver(), reactiveBlockContext(),
                () -> Traversal.traversalIterator(registry(), retrievable.pattern(), conceptMap),
                () -> RetrievableReactiveBlock.class.getSimpleName() + "(pattern: " + retrievable.pattern() +
                        ", bounds: " + conceptMap.toString() + ")"
        );
    }

    @Override
    public void resolveController(AbstractRequest<?, ?, Void> connectionRequest) {
        // Nothing to do
    }

    protected static class RetrievableReactiveBlock extends AbstractReactiveBlock<
            Void,
            ConceptMap,
            AbstractRequest<?, ?, Void>,
            RetrievableReactiveBlock
            > {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        protected RetrievableReactiveBlock(Driver<RetrievableReactiveBlock> driver,
                                           Driver<RetrievableController> controller, Context context,
                                           Supplier<FunctionalIterator<ConceptMap>> traversalSupplier,
                                           Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.traversalSupplier = traversalSupplier;
        }

        @Override
        public void setUp() {
            setOutputRouter(PoolingStream.fanOut(this));
            Source.create(this, new Operator.Supplier<>(traversalSupplier)).registerSubscriber(outputRouter());
        }
    }
}
