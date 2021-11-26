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


public abstract class Controller<CID, PID, OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>, CONTROLLER extends Controller<CID, PID, OUTPUT, PROCESSOR, CONTROLLER>> extends Actor<CONTROLLER> {

    private final ActorExecutorGroup executorService;
    private final Map<PID, Actor.Driver<PROCESSOR>> processors;


    protected Controller(Driver<CONTROLLER> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name);
        this.executorService = executorService;
        this.processors = new HashMap<>();
    }

    private CID id() {
        return null;  // TODO: provide via constructor
    }

    Actor.Driver<PROCESSOR> buildProcessor(PID id) {
        Actor.Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(), executorService);
        processors.put(id, processor);
        return processor;
    }

    abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc();

    protected abstract class UpstreamHandler<UPS_CID, UPS_PID, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, ?, UPS_CONTROLLER>> {

        private final Map<Pair<UPS_CID, UPS_PID>, Driver<PROCESSOR>> processorRequesters;

        UpstreamHandler() {
            this.processorRequesters = new HashMap<>();
        }

        protected abstract Driver<UPS_CONTROLLER> getControllerForId(UPS_CID id);  // TODO: Looks up the downstream controller by (pattern, bounds), either via registry or has already stored them.
    }

    // TODO: Child classes implement a retrieval mechanism based on the type of upstream id to find a handler for it.
    // TODO: the casting required here can't be done without the framework having knowledge of the identifier types, this needs solving
    protected abstract <UPS_CID, UPS_PID, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, ?, UPS_CONTROLLER>> UpstreamHandler<UPS_CID, UPS_PID, UPS_CONTROLLER> getHandler(UPS_CID id);

    public <UPS_CID, UPS_PID, UPS_OUTPUT, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, UPS_OUTPUT, ?, UPS_CONTROLLER>> void receiveUpstreamProcessorRequest(UPS_CID controllerId, UPS_PID processorId, Driver<PROCESSOR> requester) {
        // TODO: How do I initialise this controller to be aware of multiple other types of controller? The processor
        //  also needs to be aware in this way. e.g. it needs to be given 4 resolvable controllers of different types
        //  if its a conjunction, and a condition and a lease controller if its a conclusion
        UpstreamHandler<UPS_CID, UPS_PID, UPS_CONTROLLER> handler = getHandler(controllerId);

        // Message downstream controller responsible for creating processors as per the id.
        Driver<UPS_CONTROLLER> controller = handler.getControllerForId(controllerId);
        handler.processorRequesters.put(new Pair<>(controllerId, processorId), requester);
        sendProcessorRequest(processorId, controller);
    }

    private <UPS_CID, UPS_PID, UPS_OUTPUT, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, UPS_OUTPUT, ?, UPS_CONTROLLER>> void sendProcessorRequest(UPS_PID processorId, Driver<UPS_CONTROLLER> controller) {
        controller.execute(actor -> actor.receiveProcessorRequest(processorId, driver()));
    }

    public <REQ_CID, REQ_PID, UPS_OUTPUT, REQ_CONTROLLER extends Controller<REQ_CID, REQ_PID, UPS_OUTPUT, ?, REQ_CONTROLLER>> void receiveProcessorRequest(PID processorId, Driver<REQ_CONTROLLER> requester) {
        sendRequestedProcessor(processorId, requester, processors.computeIfAbsent(processorId, this::buildProcessor));
    }

    private <REQ_CID, REQ_PID, UPS_OUTPUT, REQ_CONTROLLER extends Controller<REQ_CID, REQ_PID, UPS_OUTPUT, ?, REQ_CONTROLLER>> void sendRequestedProcessor(PID processorId, Driver<REQ_CONTROLLER> requester, Driver<PROCESSOR> processor) {
        requester.execute(actor -> actor.receiveRequestedProcessor(id(), processorId, processor));
    }

    public <UPS_CID, UPS_PID, INLET_INPUT, UPS_PROCESSOR extends Processor<INLET_INPUT, UPS_PROCESSOR>> void receiveRequestedProcessor(UPS_CID controllerId, UPS_PID processorId, Driver<UPS_PROCESSOR> processor) {
        UpstreamHandler<UPS_CID, UPS_PID, ?> handler = getHandler(controllerId);
        Driver<PROCESSOR> requester = handler.processorRequesters.remove(new Pair<>(controllerId, processorId));
        assert requester != null;
        requester.execute(actor -> {
            Processor<?, PROCESSOR>.InletManager<UPS_PID, INLET_INPUT, UPS_PROCESSOR> inletManager = actor.getInletManager(controllerId);
            inletManager.newInlet(processorId, processor);
        });
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
