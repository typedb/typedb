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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.BufferedFanOutReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.Source;
import com.vaticle.typedb.core.reasoner.utils.Traversal;

import java.util.function.Function;
import java.util.function.Supplier;

public class RetrievableController extends Controller<ConceptMap, Void, ConceptMap,
        RetrievableController.RetrievableProcessor, RetrievableController> {

    private final Retrievable retrievable;
    private final Registry registry;

    public RetrievableController(Driver<RetrievableController> driver, String name, Retrievable retrievable,
                                 ActorExecutorGroup executorService, Registry registry) {
        super(driver, executorService, registry, name);
        this.retrievable = retrievable;
        this.registry = registry;
    }

    @Override
    public void setUpUpstreamProviders() {
        // None to set up
    }

    @Override
    protected Function<Driver<RetrievableProcessor>, RetrievableProcessor> createProcessorFunc(ConceptMap conceptMap) {
        return driver -> new RetrievableProcessor(
                driver, driver(), () -> Traversal.traversalIterator(registry, retrievable.pattern(), conceptMap),
                RetrievableProcessor.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ", bounds: " + conceptMap.toString() + ")"
        );
    }

    @Override
    public RetrievableController asController() {
        return this;
    }

    protected static class RetrievableProcessor extends Processor<Void, ConceptMap, RetrievableController, RetrievableProcessor> {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        protected RetrievableProcessor(Driver<RetrievableProcessor> driver, Driver<RetrievableController> controller,
                                       Supplier<FunctionalIterator<ConceptMap>> traversalSupplier, String name) {
            super(driver, controller, name);
            this.traversalSupplier = traversalSupplier;
        }

        @Override
        public void setUp() {
            setOutlet(new BufferedFanOutReactive<>(monitoring(), name()));
            new Source<>(traversalSupplier, monitoring(), name()).publishTo(outlet());
        }
    }
}
