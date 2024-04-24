/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;
import java.util.function.Supplier;

public class NestedConjunctionController extends ConjunctionController<
        NestedConjunctionController,
        NestedConjunctionController.NestedConjunctionProcessor
        > {

    public NestedConjunctionController(Driver<NestedConjunctionController> driver, ResolvableConjunction conjunction,
                                       Set<Identifier.Variable.Retrievable> outputVariables,
                                       Context context) {
        super(driver, conjunction, outputVariables, context);
    }

    @Override
    protected NestedConjunctionProcessor createProcessorFromDriver(
            Driver<NestedConjunctionProcessor> processorDriver,
            ConceptMap bounds
    ) {
        return new NestedConjunctionProcessor(
                processorDriver, driver(), processorContext(), bounds, getPlan(bounds.concepts().keySet()),
                () -> NestedConjunctionProcessor.class.getSimpleName() + "(pattern: " + conjunction + ", bounds: " + bounds + ")"
        );
    }

    protected static class NestedConjunctionProcessor
            extends ConjunctionController.Processor<NestedConjunctionProcessor> {

        private NestedConjunctionProcessor(Driver<NestedConjunctionProcessor> driver,
                                           Driver<NestedConjunctionController> controller, Context context,
                                           ConceptMap bounds, ConjunctionStreamPlan plan,
                                           Supplier<String> debugName) {
            super(driver, controller, context, bounds, plan, debugName);
        }

        @Override
        public void setUp() {
            Processor<NestedConjunctionProcessor>.CompoundStream conjunctionStream = new CompoundStream(this, plan, bounds);
            PoolingStream.BufferedFanStream<ConceptMap> bufferedFanStream = PoolingStream.BufferedFanStream.fanInFanOut(this);
            conjunctionStream.registerSubscriber(bufferedFanStream);
            setHubReactive(bufferedFanStream);
        }
    }

}
