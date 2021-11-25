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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;


public abstract class Controller<INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Outlet<OUTPUT>, INLET_CONTROLLER extends Controller.InletController<INPUT, INLET>, OUTLET_CONTROLLER extends Controller.OutletController<OUTPUT, OUTLET>> extends Actor<Controller<INPUT, OUTPUT, INLET, OUTLET, INLET_CONTROLLER, OUTLET_CONTROLLER>> {

    private final ActorExecutorGroup executorService;
    private final Map<PROCESSOR_ID, Actor.Driver<PROCESSOR>> processors;


    protected Controller(Driver<CONTROLLER> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name);
        this.executorService = executorService;
        this.processors = new HashMap<>();
    }

    private CONTROLLER_ID id() {
        return null;  // TODO: provide via constructor
    }

    Actor.Driver<PROCESSOR> buildProcessor(PROCESSOR_ID id) {
        Actor.Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(), executorService);
        processors.put(id, processor);
        return processor;
    }

    abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc();

    protected abstract class UpstreamHandler<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> {

        private final Map<Pair<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID>, Driver<PROCESSOR>> processorRequesters;

        UpstreamHandler() {
            this.processorRequesters = new HashMap<>();
        }

        protected abstract Driver<? extends Controller<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID, ?, ?, ?>> getControllerForId(UPSTREAM_CONTROLLER_ID id);  // TODO: Looks up the downstream controller by (pattern, bounds), either via registry or has already stored them.
    }

    // TODO: Child classes implement a retrieval mechanism based on the type of upstream id to find a handler for it.
    // TODO: the casting required here can't be done without the framework having knowledge of the identifier types, this needs solving
    protected abstract <UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> UpstreamHandler<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> getHandler(UPSTREAM_CONTROLLER_ID id);

    public <UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> void receiveUpstreamProcessorRequest(UPSTREAM_CONTROLLER_ID controllerId, UPSTREAM_PROCESSOR_ID processorId, Driver<PROCESSOR> requester) {
        // TODO: How do I initialise this controller to be aware of multiple other types of controller? The processor
        //  also needs to be aware in this way. e.g. it needs to be given 4 resolvable controllers of different types
        //  if its a conjunction, and a condition and a lease controller if its a conclusion
        UpstreamHandler<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> handler = getHandler(controllerId);

        // Message downstream controller responsible for creating processors as per the id.
        Driver<? extends Controller<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID, ?, ?, ?>> controller = handler.getControllerForId(controllerId);
        handler.processorRequesters.put(new Pair<>(controllerId, processorId), requester);
        sendProcessorRequest(processorId, controller);
    }

    private <UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> void sendProcessorRequest(UPSTREAM_PROCESSOR_ID processorId, Driver<? extends Controller<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID, ?, ?, ?>> controller) {
        controller.execute(actor -> actor.receiveProcessorRequest(processorId, driver()));
    }

    public void receiveProcessorRequest(PROCESSOR_ID processorId, Driver<? extends Controller<?, ?, ?, ?, ?>> requester) {
        sendRequestedProcessor(processorId, requester, processors.computeIfAbsent(processorId, this::buildProcessor));
    }

    private void sendRequestedProcessor(PROCESSOR_ID processorId, Driver<? extends Controller<?, ?, ?, ?, ?>> requester, Driver<PROCESSOR> processor) {
        requester.execute(actor -> actor.receiveRequestedProcessor(id(), processorId, processor));
    }

    public <UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID, UPSTREAM_PROCESSOR extends Processor<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID, ?, UPSTREAM_PROCESSOR>> void receiveRequestedProcessor(UPSTREAM_CONTROLLER_ID controllerId, UPSTREAM_PROCESSOR_ID processorId, Driver<UPSTREAM_PROCESSOR> processor) {
        UpstreamHandler<UPSTREAM_CONTROLLER_ID, UPSTREAM_PROCESSOR_ID> handler = getHandler(controllerId);
        Driver<PROCESSOR> requester = handler.processorRequesters.remove(new Pair<>(controllerId, processorId));
        assert requester != null;
        requester.execute(actor -> actor.getInletManager(controllerId).newInlet(processorId, processor));
    }

    static class Source<INPUT> {
        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<ConceptMap>> traversal) {
            return null;  // TODO
        }

        public Operation<INPUT, INPUT> asOperation() {
            return null; // TODO
        }
    }

    @Override
    protected void exception(Throwable e) {

    }
}
