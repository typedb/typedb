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


public abstract class Controller extends Actor<Controller> {

    private final ActorExecutorGroup executorService;

    protected Controller(Driver<Controller> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name);
        this.executorService = executorService;
    }

    <INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Outlet<OUTPUT>, INLET_CONTROLLER extends InletController<INPUT, INLET>, OUTLET_CONTROLLER extends OutletController<OUTPUT, OUTLET>>
    ProcessorRef<INPUT, OUTPUT, INLET, OUTLET, INLET_CONTROLLER, OUTLET_CONTROLLER> buildProcessor(Processor.Operation<INPUT, OUTPUT> operation, INLET_CONTROLLER inletController, OUTLET_CONTROLLER outletController) {
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

    static abstract class InletController<R, INLET extends Inlet<R>> {
        // Has exactly one output

        public static <INPUT> DynamicMulti<INPUT> dynamicMulti() {
            return new DynamicMulti<>();
        }

        abstract INLET inlet();

        static class DynamicMulti<T> extends InletController<T, Inlet.DynamicMulti<T>> {

            private final Inlet.DynamicMulti<T> inlet;

            private DynamicMulti() {
                inlet = new Inlet.DynamicMulti<>();
            }

            @Override
            Inlet.DynamicMulti<T> inlet() {
                return inlet;
            }

            public void addPipe(ProcessorRef<T, ?, Inlet.DynamicMulti<T>, ?, ?, ?> processorRef, ProcessorRef<?, T, ?, ? extends Outlet<T>, ?, ?> newPipe) {
                processorRef.processorDriver().execute(processor -> processor.inlet().add(newPipe.processorDriver()));
            }

        }
    }

    static abstract class OutletController<R, OUTLET extends Outlet<R>> {
        public static <T> DynamicMulti<T> dynamicMulti() {
            return new DynamicMulti<>();
        }

        public static <OUTPUT> Single<OUTPUT> single() {
            return new Single<>();
        }

        abstract OUTLET outlet();

        static class DynamicMulti<T> extends OutletController<T, Outlet.DynamicMulti<T>> {

            private final Outlet.DynamicMulti<T> outlet;

            private DynamicMulti() {
                outlet = new Outlet.DynamicMulti<>();
            }

            @Override
            Outlet.DynamicMulti<T> outlet() {
                return outlet;
            }

            public void addPipe(ProcessorRef<?, T, ?, Outlet.DynamicMulti<T>, ?, ?> processorRef, ProcessorRef<T, ?, ? extends Inlet<T>, ?, ?, ?> newPipe) {
                processorRef.processorDriver().execute(processor -> processor.outlet().add(newPipe.processorDriver()));
            }
        }

        private static class Single<OUTPUT> extends OutletController<OUTPUT, Outlet.Single<OUTPUT>> {

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

    public static class ProcessorRef<
            INPUT, OUTPUT,
            INLET extends Inlet<INPUT>, OUTLET extends Outlet<OUTPUT>,
            INLET_CONTROLLER extends InletController<INPUT, INLET>, OUTLET_CONTROLLER extends OutletController<OUTPUT, OUTLET>
            > {

        private final Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver;
        private final INLET_CONTROLLER inletController;
        private final OUTLET_CONTROLLER outletController;

        public ProcessorRef(Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver, INLET_CONTROLLER inletController, OUTLET_CONTROLLER outletController) {
            this.processorDriver = processorDriver;
            this.inletController = inletController;
            this.outletController = outletController;
        }

        public Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver() {
            return processorDriver;
        }
    }

    @Override
    protected void exception(Throwable e) {

    }
}
