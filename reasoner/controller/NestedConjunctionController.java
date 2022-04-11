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
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.reactive.TransformationStream;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class NestedConjunctionController extends ConjunctionController<
        ConceptMap,
        NestedConjunctionController,
        NestedConjunctionController.NestedConjunctionReactiveBlock
        > {

    private final Driver<Monitor> monitor;

    public NestedConjunctionController(Driver<NestedConjunctionController> driver, Conjunction conjunction,
                                       ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry) {
        super(driver, conjunction, executorService, registry);
        this.monitor = monitor;
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(registry().conceptManager(), registry().logicManager()).hasNext())
                .toSet();
    }

    @Override
    protected NestedConjunctionReactiveBlock createReactiveBlockFromDriver(
            Driver<NestedConjunctionReactiveBlock> reactiveBlockDriver,
            ConceptMap bounds
    ) {
        return new NestedConjunctionReactiveBlock(
                reactiveBlockDriver, driver(), monitor, bounds, plan(),
                () -> NestedConjunctionReactiveBlock.class.getSimpleName() + "(pattern: " + conjunction + ", bounds: " + bounds + ")"
        );
    }

    protected static class NestedConjunctionReactiveBlock
            extends ConjunctionController.ReactiveBlock<ConceptMap, NestedConjunctionReactiveBlock> {

        protected NestedConjunctionReactiveBlock(Driver<NestedConjunctionReactiveBlock> driver,
                                                 Driver<NestedConjunctionController> controller,
                                                 Driver<Monitor> monitor, ConceptMap bounds, List<Resolvable<?>> plan,
                                                 Supplier<String> debugName) {
            super(driver, controller, monitor, bounds, plan, debugName);
        }

        @Override
        public void setUp() {
            setOutputRouter(TransformationStream.fanIn(this, new CompoundOperator(this, plan, bounds)).buffer());
        }
    }

}
