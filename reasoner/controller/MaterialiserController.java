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
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.controller.MaterialiserController.MaterialisationBounds;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.Map;
import java.util.function.Function;

public class MaterialiserController extends Controller<MaterialisationBounds, Either<ConceptMap, Map<Variable, Concept>>,
        MaterialiserController.MaterialiserProcessor, MaterialiserController> {
    // TODO: It would be better not to use Either, since this class only ever outputs a Map<Variable, Concept>

    public MaterialiserController(Driver<MaterialiserController> driver, String name,
                                  ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService, registry);
    }

    @Override
    protected Function<Driver<MaterialiserProcessor>, MaterialiserProcessor> createProcessorFunc(MaterialisationBounds bounds) {
        return null;
    }

    public static class MaterialiserProcessor extends Processor<Void, Either<ConceptMap, Map<Variable, Concept>>, MaterialiserProcessor> {

        protected MaterialiserProcessor(
                Driver<MaterialiserProcessor> driver,
                Driver<? extends Controller<?, ?, MaterialiserProcessor, ?>> controller,
                String name, Reactive<Either<ConceptMap, Map<Variable, Concept>>, Either<ConceptMap, Map<Variable, Concept>>> outlet) {
            super(driver, controller, name, outlet);
        }

        @Override
        public void setUp() {
            // TODO
        }
    }

    public static class MaterialisationBounds {
        private final ConceptMap conceptMap;
        private final Rule.Conclusion conclusion;

        public MaterialisationBounds(ConceptMap conceptMap, Rule.Conclusion conclusion) {
            this.conceptMap = conceptMap;
            this.conclusion = conclusion;
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        public Rule.Conclusion conclusion() {
            return conclusion;
        }
    }
}
