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

package com.vaticle.typedb.core.reasoner.controllers;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.List;
import java.util.function.Function;

public class RootConjunctionController extends ConjunctionController<RootConjunctionController, RootConjunctionController.RootConjunctionProcessor> {
    private final Subscriber<ConceptMap> reasonerEndpoint;

    public RootConjunctionController(Driver<RootConjunctionController> driver, Conjunction conjunction,
                                     ActorExecutorGroup executorService, ControllerRegistry registry,
                                     Subscriber<ConceptMap> reasonerEndpoint) {
        super(driver, conjunction, executorService, registry);
        this.reasonerEndpoint = reasonerEndpoint;
    }

    @Override
    protected Function<Driver<RootConjunctionProcessor>, RootConjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        List<Resolvable<?>> plan = plan(conjunction);
        return driver -> new RootConjunctionProcessor(driver, driver(),
                                                      "(pattern:" + conjunction + ", bounds: " + bounds + ")",
                                                      bounds, plan, reasonerEndpoint);
    }

    public static class RootConjunctionProcessor extends ConjunctionController.ConjunctionProcessor<RootConjunctionProcessor> {

        protected RootConjunctionProcessor(Driver<RootConjunctionProcessor> driver, Driver<?
                extends Controller<Resolvable<?>, ConceptMap, RootConjunctionProcessor, ?>> controller, String name,
                                           ConceptMap bounds, List<Resolvable<?>> plan,
                                           Subscriber<ConceptMap> reasonerEndpoint) {
            super(driver, controller, name, bounds, plan);
            outlet().publishTo(reasonerEndpoint);
        }
    }
}
