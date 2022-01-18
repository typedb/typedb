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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;


public abstract class Controller<
        PROC_ID, INPUT, OUTPUT,
        PROCESSOR extends Processor<INPUT, OUTPUT, ?, PROCESSOR>,
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

    public abstract void setUpUpstreamProviders();

    protected ControllerRegistry registry() {
        return registry;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc(PROC_ID id);

    public <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, INPUT, PROCESSOR, CONTROLLER, REQ>,
            PUB_C extends Controller<PUB_PROC_ID, ?, INPUT, ?, PUB_C>> void findProviderForConnection(REQ req) {
        Builder<PUB_PROC_ID, INPUT, ?, ?, ?> builder = req.getBuilder(asController());
        builder.providerController().execute(actor -> actor.makeConnection(builder));
    }

    public abstract CONTROLLER asController();

    public void makeConnection(Builder<PROC_ID, OUTPUT, ?, ?, ?> connectionBuilder) {
        computeProcessorIfAbsent(connectionBuilder.pubProcessorId())
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
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Resolver interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
    }

    public static class Builder<PUB_PROC_ID, PACKET,
            REQ extends Processor.Request<?, PUB_PROC_ID, PUB_CONTROLLER, PACKET, PROCESSOR, ?, REQ>,
            PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
            PUB_CONTROLLER extends Controller<PUB_PROC_ID, ?, PACKET, ?, PUB_CONTROLLER>> {

        private final Driver<PUB_CONTROLLER> provController;
        private final Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PUB_PROC_ID provProcessorId;

        public Builder(Driver<PUB_CONTROLLER> provController,
                       Processor.Request<?, PUB_PROC_ID, PUB_CONTROLLER, PACKET, PROCESSOR, ?, REQ> connectionRequest) {
            this.provController = provController;
            this.recProcessor = connectionRequest.recProcessor();
            this.recEndpointId = connectionRequest.recEndpointId();
            this.connectionTransforms = connectionRequest.connectionTransforms();
            this.provProcessorId = connectionRequest.pubProcessorId();
        }

        public Builder(Driver<PUB_CONTROLLER> provController, Driver<PROCESSOR> recProcessor, long recEndpointId,
                       List<Function<PACKET, PACKET>> connectionTransforms, PUB_PROC_ID provProcessorId) {
            this.provController = provController;
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.connectionTransforms = connectionTransforms;
            this.provProcessorId = provProcessorId;
        }

        public Driver<PUB_CONTROLLER> providerController() {
            return provController;
        }

        public PUB_PROC_ID pubProcessorId(){
            return provProcessorId;
        }

        public Driver<PROCESSOR> recProcessor() {
            return recProcessor;
        }

        public Builder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, PUB_CONTROLLER> withMap(Function<PACKET, PACKET> function) {
            ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(connectionTransforms);
            newTransforms.add(function);
            return new Builder<>(provController, recProcessor, recEndpointId, newTransforms, provProcessorId);
        }

        public Builder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, PUB_CONTROLLER> withNewProcessorId(PUB_PROC_ID newPID) {
            return new Builder<>(provController, recProcessor, recEndpointId, connectionTransforms, newPID);
        }

        public <PUB_PROCESSOR extends Processor<?, PACKET, ?, PUB_PROCESSOR>> Connection<PACKET, PROCESSOR, PUB_PROCESSOR> build(Driver<PUB_PROCESSOR> pubProcessor, long pubEndpointId) {
            return new Connection<>(recProcessor, pubProcessor, recEndpointId, pubEndpointId, connectionTransforms);
        }
    }
}
