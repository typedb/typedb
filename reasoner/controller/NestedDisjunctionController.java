/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
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
