/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.reactive.Source;

import java.util.function.Supplier;

import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanOut;

public class RetrievableController extends AbstractController<
        ConceptMap, Void, ConceptMap, AbstractRequest<?, ?, Void>, RetrievableController.RetrievableProcessor,
        RetrievableController
        > {

    private final Retrievable retrievable;

    RetrievableController(Driver<RetrievableController> driver, Retrievable retrievable,
                          Context context) {
        super(driver, context, () -> RetrievableController.class.getSimpleName() + "(pattern: " + retrievable + ")");
        this.retrievable = retrievable;
    }

    @Override
    public void setUpUpstreamControllers() {
        // None to set up
    }

    @Override
    protected RetrievableProcessor createProcessorFromDriver(
            Driver<RetrievableProcessor> processorDriver, ConceptMap conceptMap
    ) {
        return new RetrievableProcessor(
                processorDriver, driver(), processorContext(),
                () -> Traversal.traversalIterator(registry(), retrievable.pattern(), conceptMap),
                () -> RetrievableProcessor.class.getSimpleName() + "(pattern: " + retrievable.pattern() +
                        ", bounds: " + conceptMap.toString() + ")"
        );
    }

    @Override
    public void routeConnectionRequest(AbstractRequest<?, ?, Void> connectionRequest) {
        // Nothing to do
    }

    protected static class RetrievableProcessor extends AbstractProcessor<
                Void,
                ConceptMap,
                AbstractRequest<?, ?, Void>,
                RetrievableProcessor
                > {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        private RetrievableProcessor(Driver<RetrievableProcessor> driver,
                                     Driver<RetrievableController> controller, Context context,
                                     Supplier<FunctionalIterator<ConceptMap>> traversalSupplier,
                                     Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.traversalSupplier = traversalSupplier;
            context().perfCounters().retrievableProcessors.add(1);
        }

        @Override
        public void setUp() {
            setHubReactive(fanOut(this));
            new Source<>(this, traversalSupplier).registerSubscriber(hubReactive());
        }
    }
}
