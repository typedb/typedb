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
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public abstract class Controller<CID, PID, OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>, CONTROLLER extends Controller<CID, PID, OUTPUT, PROCESSOR, CONTROLLER>> extends Actor<CONTROLLER> {

    private final CID id;
    private final ActorExecutorGroup executorService;
    private final Map<PID, Actor.Driver<PROCESSOR>> processors;


    protected Controller(Driver<CONTROLLER> driver, String name, CID id, ActorExecutorGroup executorService) {
        super(driver, name);
        this.id = id;
        this.executorService = executorService;
        this.processors = new HashMap<>();
    }

    private Actor.Driver<PROCESSOR> buildProcessor(PID id) {
        Actor.Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(), executorService);
        processors.put(id, processor);
        return processor;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc();

    // TODO: Rename: UpstreamCommunicator/UpstreamRequester
    protected abstract class UpstreamHandler<UPS_CID, UPS_PID, UPS_OUTPUT, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, UPS_OUTPUT, UPS_PROCESSOR, UPS_CONTROLLER>, UPS_PROCESSOR extends Processor<UPS_OUTPUT, UPS_PROCESSOR>> {

        private final Map<Pair<UPS_CID, UPS_PID>, Driver<PROCESSOR>> processorRequesters;

        UpstreamHandler() {
            this.processorRequesters = new HashMap<>();
        }

        protected abstract Driver<UPS_CONTROLLER> getControllerForId(UPS_CID id);  // Looks up the downstream controller by (pattern, bounds), either via registry or has already stored them.

        public void receiveUpstreamProcessorRequest(UPS_CID controllerId, UPS_PID processorId, Driver<PROCESSOR> requester) {
            Driver<UPS_CONTROLLER> controller = getControllerForId(controllerId);
            processorRequesters.put(new Pair<>(controllerId, processorId), requester);
            sendProcessorRequest(processorId, controller);
        }

        void sendProcessorRequest(UPS_PID processorId, Driver<UPS_CONTROLLER> controller) {
            controller.execute(actor -> actor.receiveProcessorRequest(processorId, driver()));
        }

        public void receiveRequestedProcessor(UPS_CID controllerId, UPS_PID processorId, Driver<UPS_PROCESSOR> processor) {
            Driver<PROCESSOR> requester = processorRequesters.remove(new Pair<>(controllerId, processorId));
            assert requester != null;

            // TODO: Rename to sendRequestedProcessor. This shouldn't add the inlet but should just reply with the requested processor and controller and processor ids
            requester.execute(actor -> {
                Processor<?, PROCESSOR>.InletManager<UPS_PID, UPS_OUTPUT, UPS_PROCESSOR> inletManager = actor.getInletManager(controllerId);
                inletManager.newInlet(processorId, processor);
            });
        }
    }

    // Child classes implement a retrieval mechanism based on the type of upstream id to find a handler for it.
    // the casting required here can't be done without the framework having knowledge of the identifier types, this needs solving
    protected abstract <UPS_CID, UPS_PID, UPS_OUTPUT, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, UPS_OUTPUT, UPS_PROCESSOR, UPS_CONTROLLER>, UPS_PROCESSOR extends Processor<UPS_OUTPUT, UPS_PROCESSOR>> UpstreamHandler<UPS_CID, UPS_PID, UPS_OUTPUT, UPS_CONTROLLER, UPS_PROCESSOR> getUpstreamHandler(UPS_CID id);


//    private UpstreamHandler<CID, PID, OUTPUT, CONTROLLER, PROCESSOR> getUpstreamHandlerFromDownstream(CID id) {
//
//    }

    public <REQ_CID, REQ_PID, REQ_OUTPUT, REQ_CONTROLLER extends Controller<REQ_CID, REQ_PID, REQ_OUTPUT, REQ_PROCESSOR, REQ_CONTROLLER>, REQ_PROCESSOR extends Processor<REQ_OUTPUT, REQ_PROCESSOR>> void receiveProcessorRequest(PID processorId, Driver<REQ_CONTROLLER> requester) {
        Driver<PROCESSOR> processor = processors.computeIfAbsent(processorId, this::buildProcessor);
        requester.execute(actor -> {
//            UpstreamHandler<CID, PID, OUTPUT, CONTROLLER, PROCESSOR> h = actor.getUpstreamHandler(id);  // TODO: Why doesn't this work?
            actor.getUpstreamHandler(id).receiveRequestedProcessor(id, processorId, processor);
        });  // TODO: name sendRequestedProcessor
    }

    @Override
    protected void exception(Throwable e) {
        // TODO
    }
}
