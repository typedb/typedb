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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisable;
import com.vaticle.typedb.core.logic.Rule.Conclusion.Materialisation;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.Source;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.FanOutStream;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.logic.Rule.Conclusion.materialise;

public class MaterialisationController extends Controller<Materialisable, Void, Either<ConceptMap, Materialisation>,
        MaterialisationController.MaterialisationProcessor, MaterialisationController> {
    // TODO: Either here is just to match the input to ConclusionController, but this class only ever returns Materialisation

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;
    private final Driver<Monitor> monitor;

    public MaterialisationController(Driver<MaterialisationController> driver, ActorExecutorGroup executorService,
                                     Driver<Monitor> monitor, Registry registry, TraversalEngine traversalEng,
                                     ConceptManager conceptMgr) {
        super(driver, executorService, registry, MaterialisationController.class::getSimpleName);
        this.monitor = monitor;
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
    }

    @Override
    public void setUpUpstreamProviders() {
        // None to set up
    }

    @Override
    protected Function<Driver<MaterialisationProcessor>, MaterialisationProcessor> createProcessorFunc(Materialisable materialisable) {
        return driver -> new MaterialisationProcessor(
                driver, driver(), monitor, materialisable, traversalEng, conceptMgr,
                () -> MaterialisationProcessor.class.getSimpleName() + "(Materialisable: " + materialisable + ")"
        );
    }

    @Override
    public MaterialisationController asController() {
        return this;
    }

    public static class MaterialisationProcessor
            extends Processor<Void, Either<ConceptMap, Materialisation>, MaterialisationController, MaterialisationProcessor> {

        private final Materialisable materialisable;
        private final TraversalEngine traversalEng;
        private final ConceptManager conceptMgr;

        protected MaterialisationProcessor(
                Driver<MaterialisationProcessor> driver, Driver<MaterialisationController> controller,
                Driver<Monitor> monitor, Materialisable materialisable, TraversalEngine traversalEng,
                ConceptManager conceptMgr, Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.materialisable = materialisable;
            this.traversalEng = traversalEng;
            this.conceptMgr = conceptMgr;
        }

        @Override
        public void setUp() {
            setOutlet(new FanOutStream<>(this));
            new Source<>(
                    () -> materialise(materialisable, traversalEng, conceptMgr)
                            .map(Iterators::single)
                            .orElse(Iterators.empty()),
                    this
            ).map(Either::<ConceptMap, Materialisation>second).publishTo(outlet());
        }
    }
}
