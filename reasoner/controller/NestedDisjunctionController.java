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
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;

import java.util.function.Supplier;

public class NestedDisjunctionController
        extends DisjunctionController<NestedDisjunctionController.ReactiveBlock, NestedDisjunctionController>{

    private final Driver<Monitor> monitor;

    public NestedDisjunctionController(Driver<NestedDisjunctionController> driver, Disjunction disjunction,
                                       ActorExecutorGroup executorService, Driver<Monitor> monitor, Registry registry) {
        super(driver, disjunction, executorService, registry);
        this.monitor = monitor;
    }

    @Override
    protected ReactiveBlock createReactiveBlockFromDriver(
            Driver<ReactiveBlock> reactiveBlockDriver,
            ConceptMap bounds) {
        return new ReactiveBlock(
                reactiveBlockDriver, driver(), monitor, disjunction, bounds,
                () -> ReactiveBlock.class.getSimpleName() + "(pattern:" + disjunction + ", bounds: " + bounds + ")"
        );
    }

    protected static class ReactiveBlock extends DisjunctionController.ReactiveBlock<ReactiveBlock> {

        protected ReactiveBlock(Driver<ReactiveBlock> driver,
                                Driver<NestedDisjunctionController> controller,
                                Driver<Monitor> monitor, Disjunction disjunction, ConceptMap bounds,
                                Supplier<String> debugName) {
            super(driver, controller, monitor, disjunction, bounds, debugName);
        }
    }
}
