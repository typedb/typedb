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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Materialiser.Materialisation;
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;

import java.util.function.Supplier;

import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanInFanOut;

public class ConditionController extends DisjunctionController<
        Either<ConceptMap, Materialisation>,
        ConditionController.Processor,
        ConditionController
        > {
    // Either<> here is just to match the input to ConclusionController, but this class only ever returns ConceptMap

    private final Rule.Condition condition;

    ConditionController(Driver<ConditionController> driver, Rule.Condition condition, Context context) {
        super(driver, condition.disjunction(), condition.rule().conclusion().retrievableIds(), context);
        this.condition = condition;
    }

    @Override
    protected Processor createProcessorFromDriver(Driver<Processor> processorDriver,
                                                          ConceptMap bounds) {
        return new Processor(
                processorDriver, driver(), processorContext(), disjunction, bounds,
                () -> Processor.class.getSimpleName() + "(pattern: " + condition.disjunction().pattern() + ", bounds: " + bounds + ")"
        );
    }

    protected static class Processor
            extends DisjunctionController.Processor<Either<ConceptMap, Materialisation>, Processor> {

        private Processor(Driver<Processor> driver, Driver<ConditionController> controller,
                          Context context, ResolvableDisjunction disjunction, ConceptMap bounds,
                          Supplier<String> debugName) {
            super(driver, controller, context, disjunction, bounds, debugName);
        }

        @Override
        Stream<Either<ConceptMap, Materialisation>, Either<ConceptMap, Materialisation>> getOrCreateHubReactive(Stream<ConceptMap,ConceptMap> fanIn) {
            Stream<ConceptMap, Either<ConceptMap, Materialisation>> mapStream = fanIn.map(Either::first);
            PoolingStream.BufferedFanStream<Either<ConceptMap, Materialisation>> fanMap = fanInFanOut(this);
            mapStream.registerSubscriber(fanMap);
            return fanMap;
        }
    }
}
