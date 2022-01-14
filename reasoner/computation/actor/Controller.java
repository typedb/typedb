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

package com.vaticle.typedb.core.reasoner.computation.actor;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public abstract class Controller<
        PROC_ID, INPUT, OUTPUT,
        PROCESSOR extends Processor<INPUT, OUTPUT, PROCESSOR>,
        CONTROLLER extends Controller<PROC_ID, INPUT, OUTPUT, PROCESSOR, CONTROLLER>
        > extends Actor<CONTROLLER> {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    private boolean terminated;
    private final ActorExecutorGroup executorService;
    private final ControllerRegistry registry;
    protected final Map<PROC_ID, Actor.Driver<PROCESSOR>> processors;

    protected Controller(Driver<CONTROLLER> driver, String name, ActorExecutorGroup executorService,
                         ControllerRegistry registry) {
        super(driver, name);
        this.executorService = executorService;
        this.processors = new HashMap<>();
        this.terminated = false;
        this.registry = registry;
    }

    protected ControllerRegistry registry() {
        return registry;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc(PROC_ID id);

    public <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, INPUT, PROCESSOR, REQ>,
            PUB_C extends Controller<PUB_PROC_ID, ?, INPUT, ?, PUB_C>> void findProviderForConnection(REQ req) {
        Connection.Builder<PUB_PROC_ID, INPUT, ?, ?, ?> builder = createBuilder(req);
        builder.providerController().execute(actor -> actor.makeConnection(builder));
    }

    protected abstract <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, INPUT, PROCESSOR, REQ>,
            PUB_C extends Controller<PUB_PROC_ID, ?, INPUT, ?, PUB_C>> Connection.Builder<PUB_PROC_ID, INPUT, ?, ?, ?> createBuilder(REQ req);

    public void makeConnection(Connection.Builder<PROC_ID, OUTPUT, ?, ?, ?> connectionBuilder) {
        computeProcessorIfAbsent(connectionBuilder.request().pubProcessorId())
                .execute(actor -> actor.acceptConnection(connectionBuilder));
    }

    public Driver<PROCESSOR> computeProcessorIfAbsent(PROC_ID id) {
        // TODO: We can do subsumption in the subtypes here
        return processors.computeIfAbsent(id, this::buildProcessor);
    }

    private Actor.Driver<PROCESSOR> buildProcessor(PROC_ID id) {
        Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(id), executorService);
        processor.execute(Processor::setUp);
        return processor;
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

    public boolean isTerminated() {
        return terminated;
    }

    @Override
    protected void exception(Throwable e) {
        try {
            throw e;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
