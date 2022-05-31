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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Materialiser.Materialisation;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferedFanStream.fanOut;

public class ConditionController extends ConjunctionController<
        Either<ConceptMap, Materialisation>,
        ConditionController,
        ConditionController.Processor
        > {
    // Either<> here is just to match the input to ConclusionController, but this class only ever returns ConceptMap

    private final Rule.Condition condition;

    ConditionController(Driver<ConditionController> driver, Rule.Condition condition, Context context) {
        super(driver, condition.conjunction(), context);
        this.condition = condition;
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return condition.concludablesTriggeringRules(registry().conceptManager(), registry().logicManager());
    }

    @Override
    protected Processor createProcessorFromDriver(Driver<Processor> processorDriver,
                                                          ConceptMap bounds) {
        return new Processor(
                processorDriver, driver(), processorContext(), bounds, plan(),
                () -> Processor.class.getSimpleName() + "(pattern: " + condition.conjunction() + ", bounds: " + bounds + ")"
        );
    }

    protected static class Processor
            extends ConjunctionController.Processor<Either<ConceptMap, Materialisation>, Processor> {

        private Processor(Driver<Processor> driver, Driver<ConditionController> controller,
                          Context context, ConceptMap bounds, List<Resolvable<?>> plan,
                          Supplier<String> debugName) {
            super(driver, controller, context, bounds, plan, debugName);
        }

        @Override
        public void setUp() {
            setHubReactive(fanOut(this));
            new CompoundStream(this, plan, bounds)
                    .map(Either::<ConceptMap, Materialisation>first)
                    .buffer().registerSubscriber(hubReactive());
        }
    }
}
