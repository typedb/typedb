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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public abstract class Controller<CID, PID, OUTPUT,
        PROCESSOR extends Processor<OUTPUT, PROCESSOR>,
        CONTROLLER extends Controller<CID, PID, OUTPUT, PROCESSOR, CONTROLLER>> extends Actor<CONTROLLER> {

    private final CID id;
    private final ActorExecutorGroup executorService;
    private final Map<PID, Actor.Driver<PROCESSOR>> processors;


    protected Controller(Driver<CONTROLLER> driver, String name, CID id, ActorExecutorGroup executorService) {
        super(driver, name);
        this.id = id;
        this.executorService = executorService;
        this.processors = new HashMap<>();
    }

    protected CID id() {
        return id;
    }

    private Actor.Driver<PROCESSOR> buildProcessor(PID id) {
        Actor.Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(id), executorService);
        processors.put(id, processor);
        return processor;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc(PID id);

    protected abstract <
            UPS_CID, UPS_PID,
            UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<?, UPS_PROCESSOR>
            > UpstreamTransceiver<UPS_CID, UPS_PID, ?, UPS_CONTROLLER, UPS_PROCESSOR> getUpstreamTransceiver(
            UPS_CID id, @Nullable Driver<UPS_CONTROLLER> controller);  // TODO: We only need to include the controller here to propagate the generic types correctly

    protected abstract class UpstreamTransceiver<
            UPS_CID, UPS_PID, UPS_OUTPUT,
            UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, UPS_OUTPUT, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<UPS_OUTPUT, UPS_PROCESSOR>
            > {

        private final Map<Pair<UPS_CID, UPS_PID>, Driver<PROCESSOR>> processorRequesters;

        UpstreamTransceiver() {
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
            sendRequestedProcessor(controllerId, processorId, processor, requester);
        }

        private void sendRequestedProcessor(UPS_CID controllerId, UPS_PID processorId,
                                            Driver<UPS_PROCESSOR> processor, Driver<PROCESSOR> requester) {
            requester.execute(actor -> actor.receiveUpstreamProcessor(controllerId, processorId, processor));
        }
    }

    public <REQ_CONTROLLER extends Controller<?, ?, ?, ?, REQ_CONTROLLER>> void receiveProcessorRequest(
            PID processorId, Driver<REQ_CONTROLLER> requester) {
        Driver<PROCESSOR> processor = processors.computeIfAbsent(processorId, this::buildProcessor);
        sendRequestedProcessor(processorId, requester, processor);
    }

    private <REQ_CONTROLLER extends Controller<?, ?, ?, ?, REQ_CONTROLLER>> void sendRequestedProcessor(
            PID processorId, Driver<REQ_CONTROLLER> requester, Driver<PROCESSOR> processor) {
        requester.execute(actor -> actor.getUpstreamTransceiver(id, driver()).receiveRequestedProcessor(id, processorId, processor));
    }

    @Override
    protected void exception(Throwable e) {
        // TODO
    }
}
