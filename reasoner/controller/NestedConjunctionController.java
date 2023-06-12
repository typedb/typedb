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
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.List;
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
                                           ConceptMap bounds, List<Resolvable<?>> plan,
                                           Supplier<String> debugName) {
            super(driver, controller, context, bounds, plan, debugName);
        }

        @Override
        public void setUp() {
            Processor<NestedConjunctionProcessor>.CompoundStream conjunctionStream = new CompoundStream(this, plan, compoundStreamRegistry, bounds);
            PoolingStream.BufferedFanStream<ConceptMap> bufferedFanStream = PoolingStream.BufferedFanStream.fanInFanOut(this);
            conjunctionStream.registerSubscriber(bufferedFanStream);
            setHubReactive(bufferedFanStream);
        }
    }

}
