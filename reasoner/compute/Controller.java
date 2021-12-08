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

package com.vaticle.typedb.core.reasoner.compute;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest1;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public abstract class Controller<CID, PID, OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>,
        CONTROLLER extends Controller<CID, PID, OUTPUT, PROCESSOR, CONTROLLER>> extends Actor<CONTROLLER> {

    private final CID id;
    private final ActorExecutorGroup executorService;
    protected final Map<PID, Actor.Driver<PROCESSOR>> processors;

    protected Controller(Driver<CONTROLLER> driver, String name, CID id, ActorExecutorGroup executorService) {
        super(driver, name);
        this.id = id;
        this.executorService = executorService;
        this.processors = new HashMap<>();
    }

    protected CID id() {
        return id;
    }

    protected Actor.Driver<PROCESSOR> buildProcessor(PID id) {
        Actor.Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(id), executorService);
        processors.put(id, processor);
        return processor;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc(PID id);

    <PACKET, PUB_CID, PUB_PID, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>,
            PUB_CONTROLLER extends Controller<PUB_CID, PUB_PID, PACKET, PUB_PROCESSOR, PUB_CONTROLLER>>
    void findPublisherForConnection(ConnectionRequest1<PUB_CID, PUB_PID, PACKET, PROCESSOR> connectionBuilder) {
        Processor.ConnectionRequest2<PUB_PID, PACKET, ?, ?> req = addConnectionPubController(connectionBuilder);
        req.publisherController().execute(actor -> actor.findConnection(req));
    }

    protected abstract <
            PUB_CID, PUB_PID, PACKET,
            PUB_CONTROLLER extends Controller<PUB_CID, PUB_PID, PACKET, PUB_PROCESSOR, PUB_CONTROLLER>,
            PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>
            > Processor.ConnectionRequest2<PUB_PID, PACKET, PROCESSOR, PUB_CONTROLLER> addConnectionPubController(ConnectionRequest1<PUB_CID, PUB_PID, PACKET, PROCESSOR> connectionBuilder);

    void findConnection(Processor.ConnectionRequest2<PID, OUTPUT, ?, ?> connectionBuilder) {
        Driver<PROCESSOR> processor = addConnectionPubProcessor(connectionBuilder);
        processor.execute(actor -> actor.acceptConnection(connectionBuilder.addPublisherProcessor(processor)));
    }

    protected abstract Driver<PROCESSOR> addConnectionPubProcessor(Processor.ConnectionRequest2<PID, OUTPUT, ?, ?> connectionBuilder);  // TODO: This is where we can do subsumption

    @Override
    protected void exception(Throwable e) {
        // TODO
    }
}
