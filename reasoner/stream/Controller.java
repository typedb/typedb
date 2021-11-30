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

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.stream.Processor.Connection;

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


    // =================================================================================================================

    <PACKET, UPS_CID, UPS_PID, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, PACKET, UPS_PROCESSOR, UPS_CONTROLLER>> void findUpstreamConnection(Connection.Builder<PACKET, PROCESSOR, UPS_CID, UPS_PROCESSOR> connectionBuilder) {
        Driver<UPS_CONTROLLER> controller = getControllerForId(connectionBuilder.upstreamControllerId());
        controller.execute(actor -> actor.findConnection(connectionBuilder));
    }

    void findConnection(Connection.Builder<OUTPUT, ?, CID, PROCESSOR> connectionBuilder) {
        PID processorId = connectionBuilder.upstreamProcessorId();
        Driver<PROCESSOR> processor = processors.computeIfAbsent(processorId, this::buildProcessor);
        processor.execute(actor -> actor.buildConnection(connectionBuilder));
    }

    abstract <
            UPS_CID, UPS_PID, PACKET,
            UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, PACKET, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>
            > Driver<UPS_CONTROLLER> getControllerForId(UPS_CID cid);


    // =================================================================================================================

    protected abstract <
            UPS_CID, UPS_PID,
            UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<?, UPS_PROCESSOR>
            > UpstreamTransceiver<UPS_CID, UPS_PID, ?, UPS_CONTROLLER, UPS_PROCESSOR> getUpstreamTransceiver(
            UPS_CID id, @Nullable Driver<UPS_CONTROLLER> controller);  // TODO: The only reason we include the controller here to propagate the generic types correctly

    protected abstract class UpstreamTransceiver<
            UPS_CID, UPS_PID, UPS_OUTPUT,
            UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, UPS_OUTPUT, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<UPS_OUTPUT, UPS_PROCESSOR>
            > {

        UpstreamTransceiver() {

        }

        protected abstract Driver<UPS_CONTROLLER> getControllerForId(UPS_CID id);  // Looks up the downstream controller by (pattern, bounds), either via registry or has already stored them.

    }

    @Override
    protected void exception(Throwable e) {
        // TODO
    }
}
