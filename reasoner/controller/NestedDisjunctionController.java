/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Set;
import java.util.function.Supplier;

public class NestedDisjunctionController
        extends DisjunctionController<ConceptMap, NestedDisjunctionController.Processor, NestedDisjunctionController>{

    NestedDisjunctionController(Driver<NestedDisjunctionController> driver, ResolvableDisjunction disjunction, Set<Identifier.Variable.Retrievable> outputVariables,
                                Context context) {
        super(driver, disjunction, outputVariables, context);
    }

    @Override
    protected Processor createProcessorFromDriver(
            Driver<Processor> processorDriver,
            ConceptMap bounds) {
        return new Processor(
                processorDriver, driver(), processorContext(), disjunction, bounds,
                () -> Processor.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")"
        );
    }

    protected static class Processor extends DisjunctionController.Processor<ConceptMap, Processor> {

        private Processor(Driver<Processor> driver,
                          Driver<NestedDisjunctionController> controller, Context context,
                          ResolvableDisjunction disjunction, ConceptMap bounds, Supplier<String> debugName) {
            super(driver, controller, context, disjunction, bounds, debugName);
        }

        @Override
        Stream<ConceptMap, ConceptMap> getOrCreateHubReactive(Stream<ConceptMap, ConceptMap> fanIn) {
            return fanIn;
        }
    }
}
