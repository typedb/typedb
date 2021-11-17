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
import com.vaticle.typedb.core.reasoner.stream.Processor.Inlet;
import com.vaticle.typedb.core.reasoner.stream.Processor.Pipe;


public abstract class Controller {

    private final ActorExecutorGroup executorService;

    Controller(ActorExecutorGroup executorService) {
        this.executorService = executorService;
    }

    <INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Processor.Outlet<OUTPUT>> ProcessorRef<INLET, OUTLET> buildProcessor(Pipe<INPUT, OUTPUT> pipe, InletController<INLET> inletController, OutletController<OUTLET> outletController) {
        Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver = Actor.driver(driver -> new Processor<>(driver, "name", pipe, inletController.inlet(), outletController.outlet()), executorService);
        return new ProcessorRef<>(processorDriver, inletController, outletController);
    }

    protected FunctionalIterator<ConceptMap> createTraversal(ConceptMap bounds) {
        return traversalIterator(context.concludable().pattern(), bounds);
    }

    static class Source<INPUT> {
        public static <INPUT> Source<INPUT> fromIterator(FunctionalIterator<INPUT> traversal){}

        public Pipe<INPUT, INPUT> asPipe() {

        }
    }

    static abstract class InletController<INLET extends Inlet> {
        // Has exactly one output

        public static DynamicMulti dynamicMulti() {

        }

        abstract INLET inlet();

        static class DynamicMulti extends InletController<Inlet.DynamicMulti> {

            private final Inlet.DynamicMulti inlet;

            DynamicMulti() {
                inlet = new Inlet.DynamicMulti();
            }

            @Override
            Inlet.DynamicMulti inlet() {
                return inlet;
            }
        }
    }

    static abstract class OutletController<OUTLET extends Processor.Outlet> {
        public static DynamicMulti dynamicMulti() {
            return new DynamicMulti();
        }

        abstract OUTLET outlet();

        // Has exactly one input
        static class DynamicMulti extends OutletController<Processor.Outlet.DynamicMulti> {

            private final Processor.Outlet.DynamicMulti outlet;

            DynamicMulti() {
                outlet = new Processor.Outlet.DynamicMulti();
            }

            @Override
            Processor.Outlet.DynamicMulti outlet() {
                return outlet;
            }
        }

        private static class Single extends OutletController {

        }
    }

    private static class ProcessorRef<INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Processor.Outlet<OUTPUT>> {

        private final Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver;
        private final InletController<INLET> inletController;
        private final OutletController<OUTLET> outletController;

        public ProcessorRef(Actor.Driver<Processor<INPUT, OUTPUT, INLET, OUTLET>> processorDriver, InletController<INLET> inletController, OutletController<OUTLET> outletController) {
            this.processorDriver = processorDriver;
            this.inletController = inletController;
            this.outletController = outletController;
        }
    }

}
