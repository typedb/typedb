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
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.reactive.CompoundReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class RootConjunctionController extends ConjunctionController<ConceptMap, RootConjunctionController, RootConjunctionController.RootConjunctionProcessor> {
    private final Subscriber<ConceptMap> reasonerEndpoint;

    public RootConjunctionController(Driver<RootConjunctionController> driver, Conjunction conjunction,
                                     ActorExecutorGroup executorService, ControllerRegistry registry,
                                     Subscriber<ConceptMap> reasonerEndpoint) {
        super(driver, conjunction, executorService, registry);
        this.reasonerEndpoint = reasonerEndpoint;
        initialiseProviderControllers();
    }

    @Override
    protected Function<Driver<RootConjunctionProcessor>, RootConjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new RootConjunctionProcessor(
                driver, driver(), bounds, plan(), reasonerEndpoint,
                RootConjunctionProcessor.class.getSimpleName() + "(pattern:" + conjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(registry.conceptManager(), registry.logicManager()).hasNext())
                .toSet();
    }

    protected static class RootConjunctionProcessor extends ConjunctionController.ConjunctionProcessor<ConceptMap, RootConjunctionProcessor> {

        private final Subscriber<ConceptMap> reasonerEndpoint;

        protected RootConjunctionProcessor(Driver<RootConjunctionProcessor> driver,
                                           Driver<? extends Controller<?, ?, RootConjunctionProcessor, ?>> controller,
                                           ConceptMap bounds, List<Resolvable<?>> plan,
                                           Subscriber<ConceptMap> reasonerEndpoint, String name) {
            super(driver, controller, bounds, plan, name);
            this.reasonerEndpoint = reasonerEndpoint;
        }

        @Override
        public void setUp() {
            outlet().publishTo(reasonerEndpoint);
            new CompoundReactive<>(plan, this::nextCompoundLeader, ConjunctionController::merge, bounds).publishTo(outlet());
        }
    }
}