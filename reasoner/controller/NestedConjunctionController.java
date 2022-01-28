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

import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.computation.reactive.CompoundReactive;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class NestedConjunctionController extends ConjunctionController<ConceptMap, NestedConjunctionController, NestedConjunctionController.NestedConjunctionProcessor> {

    public NestedConjunctionController(Driver<NestedConjunctionController> driver, Conjunction conjunction,
                                       ActorExecutorGroup executorService, Registry registry) {
        super(driver, conjunction, executorService, registry);
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(registry().conceptManager(), registry().logicManager()).hasNext())
                .toSet();
    }

    @Override
    protected Function<Driver<NestedConjunctionController.NestedConjunctionProcessor>,
            NestedConjunctionController.NestedConjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new NestedConjunctionProcessor(
                driver, driver(), bounds, plan(),
                NestedConjunctionProcessor.class.getSimpleName() + "(pattern: " + conjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public NestedConjunctionController asController() {
        return this;
    }

    protected static class NestedConjunctionProcessor extends ConjunctionController.ConjunctionProcessor<ConceptMap, NestedConjunctionController, NestedConjunctionProcessor> {

        protected NestedConjunctionProcessor(Driver<NestedConjunctionProcessor> driver,
                                             Driver<NestedConjunctionController> controller,
                                             ConceptMap bounds, List<Resolvable<?>> plan,
                                             String name) {
            super(driver, controller, bounds, plan, name);
        }

        @Override
        public void setUp() {
            super.setUp();
            new CompoundReactive<>(plan, this::nextCompoundLeader, ConjunctionController::merge, bounds, monitoring(), name()).buffer().publishTo(outlet());
        }
    }

}
