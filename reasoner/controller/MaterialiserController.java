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

import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;

import java.util.function.Function;

public class MaterialiserController extends Controller<Void, Void, ConclusionPacket, MaterialiserController.MaterialiserProcessor, MaterialiserController> {

    protected MaterialiserController(Driver<MaterialiserController> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name, executorService);
    }

    @Override
    protected Function<Driver<MaterialiserProcessor>, MaterialiserProcessor> createProcessorFunc(ConclusionPacket id) {
        return null;
    }

    @Override
    protected Processor.ConnectionBuilder<Void, Void, ?, ?> getProviderController(Processor.ConnectionRequest<Void,
            Void, ?> connectionRequest) {
        return null;
    }

    public static class MaterialiserProcessor extends Processor<Void, ConclusionPacket, Void, MaterialiserProcessor> {

        protected MaterialiserProcessor(Driver<MaterialiserProcessor> driver, Driver<? extends Controller<Void, Void,
                ConclusionPacket, MaterialiserProcessor, ?>> controller, String name, Reactive<ConclusionPacket,
                ConclusionPacket> outlet) {
            super(driver, controller, name, outlet);
        }

        @Override
        public void setUp() {

        }
    }
}
