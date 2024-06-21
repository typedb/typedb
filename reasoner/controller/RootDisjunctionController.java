/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.processor.reactive.RootSink;
import com.vaticle.typedb.core.traversal.common.Modifiers;

import java.util.function.Supplier;

public class RootDisjunctionController
        extends DisjunctionController<ConceptMap, RootDisjunctionController.Processor, RootDisjunctionController> {

    private final Modifiers.Filter filter;
    private final boolean explain;
    private final ReasonerConsumer<ConceptMap> reasonerConsumer;


    RootDisjunctionController(Driver<RootDisjunctionController> driver, ResolvableDisjunction disjunction,
                              Modifiers.Filter filter, boolean explain,
                              Context context, ReasonerConsumer<ConceptMap> reasonerConsumer) {
        super(driver, disjunction, filter.variables(), context);
        this.filter = filter;
        this.explain = explain;
        this.reasonerConsumer = reasonerConsumer;
    }

    @Override
    public void initialise() {
        planner().planRoot(disjunction);
        setUpUpstreamControllers();
        getOrCreateProcessor(ConceptMap.EMPTY);
    }

    @Override
    protected Processor createProcessorFromDriver(Driver<Processor> processorDriver, ConceptMap bounds) {
        return new Processor(
                processorDriver, driver(), processorContext(), disjunction, bounds, filter, explain,
                reasonerConsumer,
                () -> Processor.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        if (cause != null) {
            reasonerConsumer.exception(cause);
        }
    }

    protected static class Processor extends DisjunctionController.Processor<ConceptMap, Processor> {

        private final Modifiers.Filter filter;
        private final boolean explain;
        private final ReasonerConsumer<ConceptMap> reasonerConsumer;
        private RootSink<ConceptMap> rootSink;

        private Processor(Driver<Processor> driver, Driver<RootDisjunctionController> controller,
                          Context context, ResolvableDisjunction disjunction, ConceptMap bounds,
                          Modifiers.Filter filter, boolean explain,
                          ReasonerConsumer<ConceptMap> reasonerConsumer, Supplier<String> debugName) {
            super(driver, controller, context, disjunction, bounds, debugName);
            this.filter = filter;
            this.explain = explain;
            this.reasonerConsumer = reasonerConsumer;
        }

        @Override
        public void setUp() {
            super.setUp();
            rootSink = new RootSink<>(this, reasonerConsumer);
            hubReactive().registerSubscriber(rootSink);
        }

        @Override
        Stream<ConceptMap, ConceptMap> getOrCreateHubReactive(Stream<ConceptMap, ConceptMap> fanIn) {
            // Simply here to be overridden by root disjuntion to avoid duplicating setUp
            Stream<ConceptMap, ConceptMap> op = fanIn;
            if (!explain) op = op.map(conceptMap -> conceptMap.filter(filter));
            op = op.distinct();
            return op;
        }

        @Override
        public void rootPull() {
            rootSink.pull();
        }

        @Override
        public void onFinished(Reactive.Identifier finishable) {
            assert finishable == rootSink.identifier();
            rootSink.finished();
        }
    }
}
