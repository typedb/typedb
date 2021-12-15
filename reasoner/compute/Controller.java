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
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public abstract class Controller<PUB_CID, PACKET,
        PROCESSOR extends Processor<PACKET, PUB_CID, PROCESSOR>,
        CONTROLLER extends Controller<PUB_CID, PACKET, PROCESSOR, CONTROLLER>> extends Actor<CONTROLLER> {

    private final ActorExecutorGroup executorService;
    protected final Map<PACKET, Actor.Driver<PROCESSOR>> processors;

    protected Controller(Driver<CONTROLLER> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name);
        this.executorService = executorService;
        this.processors = new HashMap<>();
    }

    protected Actor.Driver<PROCESSOR> buildProcessor(PACKET id) {
        Actor.Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(id), executorService);
        processors.put(id, processor);
        return processor;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc(PACKET id);

    void findPublisherForConnection(ConnectionRequest<PUB_CID, PACKET, PROCESSOR> connectionRequest) {
        getPublisherController(connectionRequest).publisherController().execute(
                actor -> actor.makeConnection(getPublisherController(connectionRequest)));
    }

    protected abstract ConnectionBuilder<PUB_CID, PACKET, ?, ?> getPublisherController(ConnectionRequest<PUB_CID, PACKET, ?> connectionRequest);

    void makeConnection(ConnectionBuilder<?, PACKET, ?, ?> connectionBuilder) {
        computeProcessorIfAbsent(connectionBuilder).execute(actor -> actor.acceptConnection(connectionBuilder));
    }

    protected abstract Driver<PROCESSOR> computeProcessorIfAbsent(ConnectionBuilder<?, PACKET, ?, ?> connectionBuilder);  // TODO: This is where we can do subsumption in any processor

    @Override
    protected void exception(Throwable e) {
        // TODO
    }
}
