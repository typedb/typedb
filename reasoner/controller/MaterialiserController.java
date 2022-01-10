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
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.function.Function;

public class MaterialiserController extends Controller<ConceptMap, ConclusionController.ConclusionPacket, MaterialiserController.MaterialiserProcessor, MaterialiserController> {

    public MaterialiserController(Driver<MaterialiserController> driver, String name,
                                  ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, name, executorService, registry);
    }

    @Override
    protected Function<Driver<MaterialiserProcessor>, MaterialiserProcessor> createProcessorFunc(ConceptMap conceptMap) {
        return null;
    }

    public static class MaterialiserProcessor extends Processor<Void, ConclusionController.ConclusionPacket, MaterialiserProcessor> {

        protected MaterialiserProcessor(
                Driver<MaterialiserProcessor> driver,
                Driver<? extends Controller<?, ?, MaterialiserProcessor, ?>> controller,
                String name, Reactive<ConclusionController.ConclusionPacket, ConclusionController.ConclusionPacket> outlet) {
            super(driver, controller, name, outlet);
        }

        @Override
        public void setUp() {
            // TODO
        }
    }
}
