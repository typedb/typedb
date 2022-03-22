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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.controller.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public <PROV_CID, PROV_PID, REQ extends ProviderRequest<PROV_CID, PROV_PID, INPUT, CONTROLLER>> void findProviderForRequest(REQ req) {
        if (isTerminated()) return;
        Connection.Builder<PROV_PID, INPUT> builder = req.getConnectionBuilder(getThis());
        builder.providerController().execute(actor -> actor.sendConnectionBuilder(builder));
    }

    public abstract CONTROLLER getThis();  // We need this because the processor can't access the controller actor from the driver when building a request

    public void sendConnectionBuilder(Connection.Builder<PROC_ID, OUTPUT> connectionBuilder) {
        if (isTerminated()) return;
        createProcessorIfAbsent(connectionBuilder.providerProcessorId())
                .execute(actor -> actor.acceptConnection(connectionBuilder));
    }

    public Driver<PROCESSOR> createProcessorIfAbsent(PROC_ID processorId) {
        // TODO: We can do subsumption in the subtypes here
        return processors.computeIfAbsent(processorId, this::createProcessor);
    }

    private Actor.Driver<PROCESSOR> createProcessor(PROC_ID id) {
        if (isTerminated()) return null;  // TODO: Avoid returning null
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

    public static abstract class ProviderRequest<PROV_CID, PROV_PID, PACKET, CONTROLLER extends Controller<?, PACKET, ?, ?, CONTROLLER>> {

        private final PROV_CID provControllerId;
        private final Reactive.Identifier.Input<PACKET> recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PROV_PID provProcessorId;

        protected ProviderRequest(Reactive.Identifier.Input<PACKET> recEndpointId, PROV_CID provControllerId,
                                  PROV_PID provProcessorId) {
            this.recEndpointId = recEndpointId;
            this.provControllerId = provControllerId;
            this.provProcessorId = provProcessorId;
            this.connectionTransforms = new ArrayList<>();
        }

        public abstract Connection.Builder<PROV_PID, PACKET> getConnectionBuilder(CONTROLLER controller);

        public PROV_CID providingControllerId() {
            return provControllerId;
        }

        public PROV_PID providingProcessorId() {
            return provProcessorId;
        }

        public Reactive.Identifier.Input<PACKET> receivingEndpointId() {
            return recEndpointId;
        }

        public List<Function<PACKET, PACKET>> connectionTransforms() {
            return connectionTransforms;
        }

        @Override
        public boolean equals(Object o) {
            // TODO: be wary with request equality when conjunctions are involved
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProviderRequest<?, ?, ?, ?> request = (ProviderRequest<?, ?, ?, ?>) o;
            return recEndpointId == request.recEndpointId &&
                    provControllerId.equals(request.provControllerId) &&
                    connectionTransforms.equals(request.connectionTransforms) &&
                    provProcessorId.equals(request.provProcessorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(provControllerId, recEndpointId, connectionTransforms, provProcessorId);
        }
    }
}
