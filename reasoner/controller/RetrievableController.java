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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class RetrievableController extends Controller<Void, ConceptMap, RetrievableController.RetrievableProcessor, RetrievableController> {

    private final Retrievable retrievable;
    private final ControllerRegistry registry;

    public RetrievableController(Driver<RetrievableController> driver, String name, Retrievable retrievable,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService);
        this.retrievable = retrievable;
        this.registry = registry;
    }

    @Override
    protected Function<Driver<RetrievableProcessor>, RetrievableProcessor> createProcessorFunc(ConceptMap conceptMap) {
        return driver -> new RetrievableProcessor(
                driver, driver(), processorName(conceptMap),
                () -> TraversalUtils.traversalIterator(registry, retrievable.pattern(), conceptMap)
        );
    }

    private String processorName(ConceptMap conceptMap) {
        return RetrievableProcessor.class.getSimpleName() + "(pattern: " + retrievable.pattern() + ", bounds: " + conceptMap.toString() + ")";
    }


    @Override
    protected Processor.ConnectionBuilder<Void, ConceptMap, ?, ?> getProviderController(Processor.ConnectionRequest<Void, ConceptMap, ?> connectionRequest) {
        throw TypeDBException.of(ILLEGAL_STATE);  // TODO: Can we use typing to remove this?
    }

    public static class RetrievableProcessor extends Processor<ConceptMap, Void, RetrievableProcessor> {

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSupplier;

        protected RetrievableProcessor(Driver<RetrievableProcessor> driver,
                                       Driver<? extends Controller<Void, ConceptMap, RetrievableProcessor, ?>> controller,
                                       String name, Supplier<FunctionalIterator<ConceptMap>> traversalSupplier) {
            super(driver, controller, name, noOp());
            this.traversalSupplier = traversalSupplier;
        }

        @Override
        public void setUp() {
            new Source<>(traversalSupplier).publishTo(outlet());
        }
    }
}
