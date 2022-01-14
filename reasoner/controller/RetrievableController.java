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
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class RetrievableController extends Controller<ConceptMap, Void, ConceptMap,
        RetrievableController.RetrievableProcessor, RetrievableController> {

    private final Retrievable retrievable;
    private final ControllerRegistry registry;

    public RetrievableController(Driver<RetrievableController> driver, String name, Retrievable retrievable,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService, registry);
        this.retrievable = retrievable;
        this.registry = registry;
    }

    @Override
    protected Function<Driver<RetrievableProcessor>, RetrievableProcessor> createProcessorFunc(ConceptMap conceptMap) {
        return driver -> new RetrievableProcessor(
                driver, driver(), () -> TraversalUtils.traversalIterator(registry, retrievable.pattern(), conceptMap),
                RetrievableProcessor.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ", bounds: " + conceptMap.toString() + ")"
        );
    }

    @Override
    protected <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, Void,
            RetrievableProcessor, REQ>, PUB_C extends Controller<PUB_PROC_ID, ?, Void, ?, PUB_C>> Connection.Builder<PUB_PROC_ID, Void, ?, ?, ?> createBuilder(REQ req) {
        return null;
    }

    protected static class RetrievableProcessor extends Processor<Void, ConceptMap, RetrievableProcessor> {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        protected RetrievableProcessor(Driver<RetrievableProcessor> driver,
                                       Driver<? extends Controller<?, Void, ?, RetrievableProcessor, ?>> controller,
                                       Supplier<FunctionalIterator<ConceptMap>> traversalSupplier, String name) {
            super(driver, controller, noOp(name), name);
            this.traversalSupplier = traversalSupplier;
        }

        @Override
        public void setUp() {
            new Source<>(traversalSupplier, name()).publishTo(outlet());
        }
    }
}
