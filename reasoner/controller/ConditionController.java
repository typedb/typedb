/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanInFanOut;

public class ConditionController extends DisjunctionController<
        Either<ConceptMap, Materialisation>,
        ConditionController.Processor,
        ConditionController
        > {
    // Either<> here is just to match the input to ConclusionController, but this class only ever returns ConceptMap

    private final Rule.Condition condition;

    ConditionController(Driver<ConditionController> driver, Rule.Condition condition, Context context) {
        super(driver, condition.disjunction(), iterate(condition.rule().conclusion().retrievableIds()).filter(v -> !v.isAnonymous()).toSet(), context);
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
