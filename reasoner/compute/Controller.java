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
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest2;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public abstract class Controller<CID, PID, PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR extends Processor<PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR>,
        CONTROLLER extends Controller<CID, PID, PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR, CONTROLLER>> extends Actor<CONTROLLER> {

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

    void findPublisherForConnection(ConnectionRequest1<PUB_CID, PUB_PID, INPUT, PROCESSOR> connectionBuilder) {
        ConnectionRequest2<PUB_PID, INPUT, ?, ?> req = makeConnectionRequest2(connectionBuilder);
        req.publisherController().execute(actor -> actor.makeConnection(req));
    }

    protected abstract ConnectionRequest2<PUB_PID, INPUT, PROCESSOR, ?> makeConnectionRequest2(ConnectionRequest1<PUB_CID, PUB_PID, INPUT, PROCESSOR> connectionBuilder);

    void makeConnection(ConnectionRequest2<PID, OUTPUT, ?, ?> connectionBuilder) {
        Driver<PROCESSOR> processor = addConnectionPubProcessor(connectionBuilder);
        Processor.Connection<OUTPUT, ?, PROCESSOR> connection = connectionBuilder.connect(processor);
        processor.execute(actor -> actor.acceptConnection(connection));
    }

    protected abstract Driver<PROCESSOR> addConnectionPubProcessor(ConnectionRequest2<PID, OUTPUT, ?, ?> connectionBuilder);  // TODO: This is where we can do subsumption in any processor

    @Override
    protected void exception(Throwable e) {
        // TODO
    }
}
