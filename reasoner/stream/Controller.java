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

package com.vaticle.typedb.core.reasoner.stream;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.pattern.Pattern;
import com.vaticle.typedb.core.reasoner.stream.Processor.Inlet;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;
import com.vaticle.typedb.core.reasoner.stream.Processor.Outlet;


public abstract class Controller {

    private final ActorExecutorGroup executorService;

    Controller(ActorExecutorGroup executorService) {
        this.executorService = executorService;
    }

    <INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Outlet<OUTPUT>> ProcessorRef<INPUT, OUTPUT, INLET, OUTLET> buildProcessor(Processor.Operation<INPUT, OUTPUT> operation, InletController<INLET> inletController, OutletController<OUTLET> outletController) {
        Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver = Actor.driver(driver -> new Processor<>(driver, "name", operation, inletController.inlet(), outletController.outlet()), executorService);
        return new ProcessorRef<>(processorDriver, inletController, outletController);
    }

    public FunctionalIterator<ConceptMap> createTraversal(Pattern pattern, ConceptMap bounds) {
        return null; // TODO
    }

    static class Source<INPUT> {
        public static <INPUT> Source<INPUT> fromIterator(FunctionalIterator<INPUT> traversal) {
            return null;  // TODO
        }

        public Operation<INPUT, INPUT> asOperation() {
            return null; // TODO
        }
    }

    static abstract class InletController<INLET extends Inlet<?>> {
        // Has exactly one output

        public static DynamicMulti dynamicMulti() {
            return new DynamicMulti();
        }

        abstract INLET inlet();

        static class DynamicMulti extends InletController<Inlet.DynamicMulti<?>> {

            private final Inlet.DynamicMulti<?> inlet;

            private DynamicMulti() {
                inlet = new Inlet.DynamicMulti<>();
            }

            @Override
            Inlet.DynamicMulti<?> inlet() {
                return inlet;
            }

            public <T> void addPipe(ProcessorRef<Inlet.DynamicMulti<T>, ?, ?, ?> processorRef, ProcessorRef<?, ? extends Outlet<T>, ?, ?> newPipe) {
                processorRef.processorDriver().execute(processor -> processor.inlet().add(newPipe.processorDriver()));
            }

        }
    }

    static abstract class OutletController<OUTLET extends Outlet<?>> {
        public static DynamicMulti dynamicMulti() {
            return new DynamicMulti();
        }

        public static <OUTPUT> Single<OUTPUT> single() {
            return new Single<>();
        }

        abstract OUTLET outlet();

        // Has exactly one input
        static class DynamicMulti extends OutletController<Outlet.DynamicMulti<?>> {

            private final Outlet.DynamicMulti<?> outlet;

            private DynamicMulti() {
                outlet = new Outlet.DynamicMulti<>();
            }

            @Override
            Outlet.DynamicMulti<?> outlet() {
                return outlet;
            }

            public <T> void addPipe(ProcessorRef<?, Outlet.DynamicMulti<T>, ?, ?> processorRef, ProcessorRef<? extends Inlet<T>, ?, ?, ?> newPipe) {
                processorRef.processorDriver().execute(processor -> processor.outlet().add(newPipe.processorDriver()));
            }
        }

        private static class Single<OUTPUT> extends OutletController<Outlet.Single<OUTPUT>> {

            private final Outlet.Single<OUTPUT> outlet;

            private Single() {
                this.outlet = new Outlet.Single<>();
            }

            @Override
            Outlet.Single<OUTPUT> outlet() {
                return outlet;
            }
        }
    }

    public static class ProcessorRef<INLET extends Inlet<?>, OUTLET extends Outlet<?>, INLET_CONTROLLER extends InletController<INLET>, OUTLET_CONTROLLER extends OutletController<OUTLET>> {

        private final Actor.Driver<? extends Processor<?, ?, INLET, OUTLET>> processorDriver;
        private final INLET_CONTROLLER inletController;
        private final OUTLET_CONTROLLER outletController;

        public ProcessorRef(Actor.Driver<? extends Processor<?, ?, INLET, OUTLET>> processorDriver, INLET_CONTROLLER inletController, OUTLET_CONTROLLER outletController) {
            this.processorDriver = processorDriver;
            this.inletController = inletController;
            this.outletController = outletController;
        }

        public Actor.Driver<? extends Processor<?, ?, INLET, OUTLET>> processorDriver() {
            return processorDriver;
        }
    }

    private static class Pipe {}
}
