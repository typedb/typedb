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
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.BufferBroadcastReactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Source;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import java.util.HashSet;
import java.util.function.Function;

import static com.vaticle.typedb.core.logic.Rule.Conclusion.materialise;

public class MaterialiserController extends Controller<Materialisable, Void, Either<ConceptMap, Materialisation>,
        MaterialiserController.MaterialiserProcessor, MaterialiserController> {
    // TODO: It would be better not to use Either, since this class only ever outputs a Materialisation

    private final ConceptManager conceptMgr;
    private final TraversalEngine traversalEng;

    public MaterialiserController(Driver<MaterialiserController> driver, ActorExecutorGroup executorService,
                                  Registry registry, TraversalEngine traversalEng, ConceptManager conceptMgr) {
        super(driver, executorService, registry, MaterialiserController.class.getSimpleName());
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
    }

    @Override
    public void setUpUpstreamProviders() {
        // None to set up
    }

    @Override
    protected Function<Driver<MaterialiserProcessor>, MaterialiserProcessor> createProcessorFunc(Materialisable materialisable) {
        return driver -> new MaterialiserProcessor(
                driver, driver(), materialisable, traversalEng, conceptMgr,
                MaterialiserProcessor.class.getSimpleName() + "(Materialisable: " + materialisable + ")"
        );
    }

    @Override
    public MaterialiserController asController() {
        return this;
    }

    public static class MaterialiserProcessor
            extends Processor<Void, Either<ConceptMap, Materialisation>, MaterialiserController, MaterialiserProcessor> {

        private final Materialisable materialisable;
        private final TraversalEngine traversalEng;
        private final ConceptManager conceptMgr;

        protected MaterialiserProcessor(
                Driver<MaterialiserProcessor> driver, Driver<MaterialiserController> controller,
                Materialisable materialisable, TraversalEngine traversalEng, ConceptManager conceptMgr, String name) {
            super(driver, controller, new BufferBroadcastReactive<>(new HashSet<>(), name), name);
            this.materialisable = materialisable;
            this.traversalEng = traversalEng;
            this.conceptMgr = conceptMgr;
        }

        @Override
        public void setUp() {
            new Source<>(
                    () -> materialise(materialisable, traversalEng, conceptMgr)
                            .map(Iterators::single)
                            .orElse(Iterators.empty()),
                    name()
            )
                    .map(Either::<ConceptMap, Materialisation>second)
                    .publishTo(outlet());
        }
    }
}
