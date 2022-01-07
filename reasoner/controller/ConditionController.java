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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.reactive.CompoundReactive;
import com.vaticle.typedb.core.reasoner.controller.ConclusionPacket.ConditionAnswer;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class ConditionController extends ConjunctionController<ConclusionPacket, ConditionController, ConditionController.ConditionProcessor> {

    public ConditionController(Driver<ConditionController> driver, Conjunction conjunction, ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, conjunction, executorService, registry);
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return null;
    }

    @Override
    protected Function<Driver<ConditionController.ConditionProcessor>, ConditionController.ConditionProcessor> createProcessorFunc(ConceptMap id) {
        return null;
    }

    protected static class ConditionProcessor extends ConjunctionController.ConjunctionProcessor<ConclusionPacket, ConditionController.ConditionProcessor>{
        protected ConditionProcessor(Driver<ConditionProcessor> driver, Driver<? extends Controller<?, ?,
                ConditionProcessor, ?>> controller, String name,
                                     ConceptMap bounds, List<Resolvable<?>> plan) {
            super(driver, controller, name, bounds, plan);
        }

        @Override
        public void setUp() {
            new CompoundReactive<>(plan, this::nextCompoundLeader, ConjunctionController::merge, bounds)
                    .map(a -> new ConditionAnswer(a).asConclusionPacket()).publishTo(outlet());
        }
    }
}
