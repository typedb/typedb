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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class NestedConjunctionController extends ConjunctionController<
        ConceptMap,
        NestedConjunctionController,
        NestedConjunctionController.NestedConjunctionProcessor
        > {

    public NestedConjunctionController(Driver<NestedConjunctionController> driver, Conjunction conjunction,
                                       Context context) {
        super(driver, conjunction, context);
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(registry().conceptManager(), registry().logicManager()).hasNext())
                .toSet();
    }

    @Override
    protected NestedConjunctionProcessor createProcessorFromDriver(
            Driver<NestedConjunctionProcessor> processorDriver,
            ConceptMap bounds
    ) {
        return new NestedConjunctionProcessor(
                processorDriver, driver(), processorContext(), bounds, plan(),
                () -> NestedConjunctionProcessor.class.getSimpleName() + "(pattern: " + conjunction + ", bounds: " + bounds + ")"
        );
    }

    protected static class NestedConjunctionProcessor
            extends ConjunctionController.Processor<ConceptMap, NestedConjunctionProcessor> {

        private NestedConjunctionProcessor(Driver<NestedConjunctionProcessor> driver,
                                           Driver<NestedConjunctionController> controller, Context context,
                                           ConceptMap bounds, List<Resolvable<?>> plan,
                                           Supplier<String> debugName) {
            super(driver, controller, context, bounds, plan, debugName);
        }

        @Override
        public void setUp() {
            setHubReactive(new CompoundStream(this, plan, bounds).buffer());
        }
    }

}
