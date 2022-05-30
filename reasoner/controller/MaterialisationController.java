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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Materialiser;
import com.vaticle.typedb.core.logic.Materialiser.Materialisation;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisable;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Source;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.function.Supplier;

public class MaterialisationController extends AbstractController<
        Materialisable, Void, Either<ConceptMap, Materialisation>, AbstractRequest<?, ?, Void, ?>,
        MaterialisationController.Processor, MaterialisationController
        > {
    // Either<> is just to match the input to ConclusionController, but this class only ever returns Materialisation

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;

    MaterialisationController(Driver<MaterialisationController> driver, Context context,
                              TraversalEngine traversalEng, ConceptManager conceptMgr) {
        super(driver, context, MaterialisationController.class::getSimpleName);
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
    }

    @Override
    public void setUpUpstreamControllers() {
        // None to set up
    }

    @Override
    protected Processor createProcessorFromDriver(
            Driver<Processor> processorDriver, Materialisable materialisable
    ) {
        return new Processor(
                processorDriver, driver(), processorContext(), materialisable, traversalEng, conceptMgr,
                () -> Processor.class.getSimpleName() + "(Materialisable: " + materialisable + ")"
        );
    }

    @Override
    public void routeConnectionRequest(AbstractRequest<?, ?, Void, ?> connectionRequest) {
        // Nothing to do
    }

    public static class Processor extends AbstractProcessor<Void, Either<ConceptMap, Materialisation>,
                AbstractRequest<?, ?, Void, ?>, Processor> {

        private final Materialisable materialisable;
        private final TraversalEngine traversalEng;
        private final ConceptManager conceptMgr;

        private Processor(Driver<Processor> driver, Driver<MaterialisationController> controller,
                          Context context, Materialisable materialisable, TraversalEngine traversalEng,
                          ConceptManager conceptMgr, Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.materialisable = materialisable;
            this.traversalEng = traversalEng;
            this.conceptMgr = conceptMgr;
        }

        @Override
        public void setUp() {
            setHubReactive(PoolingStream.fanOut(this));
            Source.create(this,
                          () -> Materialiser.materialise(materialisable, traversalEng, conceptMgr)
                                  .map(Iterators::single)
                                  .orElse(Iterators.empty())
            ).map(Either::<ConceptMap, Materialisation>second).registerSubscriber(outputRouter());
        }
    }
}
