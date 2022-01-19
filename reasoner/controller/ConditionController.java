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
import com.vaticle.typedb.core.reasoner.computation.reactive.CompoundReactive;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class ConditionController extends ConjunctionController<Either<ConceptMap, Materialisation>, ConditionController, ConditionController.ConditionProcessor> {
    // TODO: It would be better not to use Either, since this class only ever outputs a ConceptMap

    private final Rule.Condition condition;

    public ConditionController(Driver<ConditionController> driver, Rule.Condition condition, ActorExecutorGroup executorService, Registry registry) {
        super(driver, condition.conjunction(), executorService, registry);
        this.condition = condition;
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return condition.concludablesTriggeringRules(registry().conceptManager(), registry().logicManager());
    }

    @Override
    protected Function<Driver<ConditionController.ConditionProcessor>, ConditionController.ConditionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new ConditionProcessor(
                driver, driver(), bounds, plan(),
                ConditionProcessor.class.getSimpleName() + "(pattern: " + condition.conjunction() + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public ConditionController asController() {
        return this;
    }

    protected static class ConditionProcessor extends ConjunctionController.ConjunctionProcessor<Either<ConceptMap, Materialisation>, ConditionController, ConditionProcessor>{
        protected ConditionProcessor(Driver<ConditionProcessor> driver, Driver<ConditionController> controller,
                                     ConceptMap bounds, List<Resolvable<?>> plan, String name) {
            super(driver, controller, bounds, plan, name);
        }

        @Override
        public void setUp() {
            super.setUp();
            new CompoundReactive<>(plan, this::nextCompoundLeader, ConjunctionController::merge, bounds, this, name())
                    .map(Either::<ConceptMap, Materialisation>first).publishTo(outlet());
        }
    }
}
