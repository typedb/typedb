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
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisation;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.reactive.TransformationStream;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class ConditionController extends ConjunctionController<Either<ConceptMap, Materialisation>, ConditionController, ConditionController.ConditionReactiveBlock> {
    // TODO: Either here is just to match the input to ConclusionController, but this class only ever returns ConceptMap

    private final Rule.Condition condition;
    private final Driver<Monitor> monitor;

    public ConditionController(Driver<ConditionController> driver, Rule.Condition condition,
                               ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry) {
        super(driver, condition.conjunction(), executorService, registry);
        this.condition = condition;
        this.monitor = monitor;
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return condition.concludablesTriggeringRules(registry().conceptManager(), registry().logicManager());
    }

    @Override
    protected ConditionReactiveBlock createReactiveBlockFromDriver(Driver<ConditionReactiveBlock> reactiveBlockDriver, ConceptMap bounds) {
        return new ConditionReactiveBlock(
                reactiveBlockDriver, driver(), monitor, bounds, plan(),
                () -> ConditionReactiveBlock.class.getSimpleName() + "(pattern: " + condition.conjunction() + ", bounds: " + bounds + ")"
        );
    }

    protected static class ConditionReactiveBlock extends ConjunctionController.ConjunctionReactiveBlock<Either<ConceptMap, Materialisation>, ConditionReactiveBlock>{
        protected ConditionReactiveBlock(Driver<ConditionReactiveBlock> driver, Driver<ConditionController> controller,
                                     Driver<Monitor> monitor, ConceptMap bounds, List<Resolvable<?>> plan,
                                     Supplier<String> debugName) {
            super(driver, controller, monitor, bounds, plan, debugName);
        }

        @Override
        public void setUp() {
            setOutputRouter(
                    TransformationStream.fanIn(this, new CompoundOperator(this, plan, bounds))
                            .map(Either::<ConceptMap, Materialisation>first).buffer()
            );
        }
    }
}
