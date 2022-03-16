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
import com.vaticle.typedb.core.reasoner.controller.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;


public abstract class Controller<
        PROC_ID, INPUT, OUTPUT,
        PROCESSOR extends Processor<INPUT, OUTPUT, ?, PROCESSOR>,
        CONTROLLER extends Controller<PROC_ID, INPUT, OUTPUT, PROCESSOR, CONTROLLER>
        > extends Actor<CONTROLLER> {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    private boolean terminated;
    private final ActorExecutorGroup executorService;
    private final Registry registry;
    protected final Map<PROC_ID, Actor.Driver<PROCESSOR>> processors;

    protected Controller(Driver<CONTROLLER> driver, ActorExecutorGroup executorService, Registry registry,
                         Supplier<String> debugName) {
        super(driver, debugName);
        this.executorService = executorService;
        this.processors = new HashMap<>();
        this.terminated = false;
        this.registry = registry;
    }

    public abstract void setUpUpstreamProviders();

    protected Registry registry() {
        return registry;
    }

    protected abstract Function<Driver<PROCESSOR>, PROCESSOR> createProcessorFunc(PROC_ID id);

    public <PROV_CID, PROV_PROC_ID, REQ extends Processor.Request<PROV_CID, PROV_PROC_ID, PROV_C, INPUT, PROCESSOR, CONTROLLER, REQ>,
            PROV_C extends Controller<PROV_PROC_ID, ?, INPUT, ?, PROV_C>> void findProviderForConnection(REQ req) {
        Builder<PROV_PROC_ID, INPUT, ?, ?, ?> builder = req.getBuilder(asController());
        if (isTerminated()) return;
        builder.providerController().execute(actor -> actor.makeConnection(builder));
    }

    public abstract CONTROLLER asController();

    public void makeConnection(Builder<PROC_ID, OUTPUT, ?, ?, ?> connectionBuilder) {
        if (isTerminated()) return;
        computeProcessorIfAbsent(connectionBuilder.providingProcessorId())
                .execute(actor -> actor.acceptConnection(connectionBuilder));
    }

    public Driver<PROCESSOR> computeProcessorIfAbsent(PROC_ID id) {
        // TODO: We can do subsumption in the subtypes here
        return processors.computeIfAbsent(id, this::buildProcessor);
    }

    private Actor.Driver<PROCESSOR> buildProcessor(PROC_ID id) {
        if (isTerminated()) return null;
        Driver<PROCESSOR> processor = Actor.driver(createProcessorFunc(id), executorService);
        processor.execute(Processor::setUp);
        return processor;
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Controller interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            } else {
                LOG.debug("Controller interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
        processors.values().forEach(p -> p.execute(actor -> actor.terminate(cause)));
    }

    public boolean isTerminated() {
        return terminated;
    }

    public static class Builder<PROV_PROC_ID, PACKET,
            REQ extends Processor.Request<?, PROV_PROC_ID, PROV_CONTROLLER, PACKET, PROCESSOR, ?, REQ>,
            PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
            PROV_CONTROLLER extends Controller<PROV_PROC_ID, ?, PACKET, ?, PROV_CONTROLLER>> {

        private final Driver<PROV_CONTROLLER> provController;
        private final Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PROV_PROC_ID provProcessorId;

        public Builder(Driver<PROV_CONTROLLER> provController,
                       Processor.Request<?, PROV_PROC_ID, PROV_CONTROLLER, PACKET, PROCESSOR, ?, REQ> connectionRequest) {
            this.provController = provController;
            this.recProcessor = connectionRequest.receivingProcessor();
            this.recEndpointId = connectionRequest.receivingEndpointId();
            this.connectionTransforms = connectionRequest.connectionTransforms();
            this.provProcessorId = connectionRequest.providingProcessorId();
        }

        public Builder(Driver<PROV_CONTROLLER> provController, Driver<PROCESSOR> recProcessor, long recEndpointId,
                       List<Function<PACKET, PACKET>> connectionTransforms, PROV_PROC_ID provProcessorId) {
            this.provController = provController;
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.connectionTransforms = connectionTransforms;
            this.provProcessorId = provProcessorId;
        }

        public Driver<PROV_CONTROLLER> providerController() {
            return provController;
        }

        public PROV_PROC_ID providingProcessorId(){
            return provProcessorId;
        }

        public Driver<PROCESSOR> receivingProcessor() {
            return recProcessor;
        }

        public Builder<PROV_PROC_ID, PACKET, REQ, PROCESSOR, PROV_CONTROLLER> withMap(Function<PACKET, PACKET> function) {
            ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(connectionTransforms);
            newTransforms.add(function);
            return new Builder<>(provController, recProcessor, recEndpointId, newTransforms, provProcessorId);
        }

        public Builder<PROV_PROC_ID, PACKET, REQ, PROCESSOR, PROV_CONTROLLER> withNewProcessorId(PROV_PROC_ID newPID) {
            return new Builder<>(provController, recProcessor, recEndpointId, connectionTransforms, newPID);
        }

        public <PROV_PROCESSOR extends Processor<?, PACKET, ?, PROV_PROCESSOR>> Connection<PACKET, PROCESSOR,
                PROV_PROCESSOR> build(Driver<PROV_PROCESSOR> pubProcessor, long pubEndpointId) {
            return new Connection<>(recProcessor, pubProcessor, recEndpointId, pubEndpointId, connectionTransforms);
        }
    }
}
