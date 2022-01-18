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
import com.vaticle.typedb.core.reasoner.computation.reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.PublisherImpl;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class Processor<INPUT, OUTPUT,
        CONTROLLER extends Controller<?, INPUT, OUTPUT, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<INPUT, OUTPUT, ?, PROCESSOR>> extends Actor<PROCESSOR> {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    private final Driver<CONTROLLER> controller;
    private final Reactive<OUTPUT, OUTPUT> outlet;
    private final Map<Long, InletEndpoint<INPUT>> receivingEndpoints;
    private final Map<Long, OutletEndpoint<OUTPUT>> providingEndpoints;
    private long endpointId;

    protected Processor(Driver<PROCESSOR> driver,
                        Driver<CONTROLLER> controller,
                        Reactive<OUTPUT, OUTPUT> outlet, String name) {
        super(driver, name);
        this.controller = controller;
        this.outlet = outlet;
        this.endpointId = 0;
        this.receivingEndpoints = new HashMap<>();
        this.providingEndpoints = new HashMap<>();
    }

    public abstract void setUp();

    public Reactive<OUTPUT, OUTPUT> outlet() {
        return outlet;
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Resolver interrupted by resource close: {}", e.getMessage());
                controller.execute(actor -> actor.exception(e));
                return;
            }
        }
        LOG.error("Actor exception", e);
        controller.execute(actor -> actor.exception(e));
    }

    protected <PUB_CID, PUB_PROC_ID,
            REQ extends Request<PUB_CID, PUB_PROC_ID, PUB_C, INPUT, PROCESSOR, CONTROLLER, REQ>,
            PUB_C extends Controller<PUB_PROC_ID, ?, INPUT, ?, PUB_C>
            > void requestConnection(REQ req) {
        controller.execute(actor -> actor.findProviderForConnection(req));
    }

    protected void acceptConnection(Controller.Builder<?, OUTPUT, ?, ?, ?> connectionBuilder) {
        Connection<OUTPUT, ?, PROCESSOR> connection = connectionBuilder.build(driver(), nextEndpointId());
        applyConnectionTransforms(connection.transformations(), outlet(), createProvidingEndpoint(connection));
        connectionBuilder.recProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    public void applyConnectionTransforms(List<Function<OUTPUT, OUTPUT>> transformations,
                                          Reactive<OUTPUT, OUTPUT> outlet, OutletEndpoint<OUTPUT> upstreamEndpoint) {
        Provider.Publisher<OUTPUT> op = outlet;
        for (Function<OUTPUT, OUTPUT> t : transformations) op = op.map(t);
        op.publishTo(upstreamEndpoint);
    }

    protected <PUB_PROCESSOR extends Processor<?, INPUT, ?, PUB_PROCESSOR>> void finaliseConnection(Connection<INPUT, ?, PUB_PROCESSOR> connection) {
        receivingEndpoints.get(connection.subEndpointId()).setReady(connection);
    }

    private long nextEndpointId() {
        endpointId += 1;
        return endpointId;
    }

    protected OutletEndpoint<OUTPUT> createProvidingEndpoint(Connection<OUTPUT, ?, PROCESSOR> connection) {
        OutletEndpoint<OUTPUT> endpoint = new OutletEndpoint<>(connection, name());
        providingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected InletEndpoint<INPUT> createReceivingEndpoint() {
        InletEndpoint<INPUT> endpoint = new InletEndpoint<>(nextEndpointId(), name());
        receivingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected void endpointPull(long pubEndpointId) {
        providingEndpoints.get(pubEndpointId).pull(null);
    }

    protected void endpointReceive(INPUT packet, long subEndpointId) {
        receivingEndpoints.get(subEndpointId).receive(null, packet);
    }

    /**
     * Governs an input to a processor
     */
    public static class InletEndpoint<PACKET> extends PublisherImpl<PACKET> implements Receiver<PACKET> {

        private final long id;
        private boolean ready;
        private Connection<PACKET, ?, ?> connection;
        protected boolean isPulling;

        public InletEndpoint(long id, String groupName) {
            super(groupName);
            this.id = id;
            this.ready = false;
            this.isPulling = false;
        }

        public long id() {
            return id;
        }

        void setReady(Connection<PACKET, ?, ?> connection) {
            this.connection = connection;
            this.ready = true;
            if (isPulling) this.pull(subscriber());
        }

        @Override
        public void pull(Receiver<PACKET> receiver) {
            assert receiver.equals(subscriber);
            ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, this));
            isPulling = true;
            if (ready) {
                connection.pull();
                ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, connection));  // TODO: We do this here because we don't tell the connection who we are when we pull
            }
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.receive(connection, this, packet));  // TODO: Highlights a smell that the connection is receiving and so provider is null
            assert provider == null;
            isPulling = false;
            subscriber().receive(this, packet);
        }
    }

    /**
     * Governs an output from a processor
     */
    public static class OutletEndpoint<PACKET> implements Subscriber<PACKET>, Provider<PACKET> {

        private final Connection<PACKET, ?, ?> connection;
        private final Set<Provider<PACKET>> publishers;
        protected boolean isPulling;
        private final String groupName;

        public OutletEndpoint(Connection<PACKET, ?, ?> connection, String groupName) {
            this.groupName = groupName;
            this.publishers = new HashSet<>();
            this.connection = connection;
            this.isPulling = false;
        }

        public long id() {
            return connection.providingEndpointId();
        }

        @Override
        public String groupName() {
            return groupName;
        }

        @Override
        public void subscribeTo(Provider<PACKET> publisher) {
            publishers.add(publisher);
            if (isPulling) publisher.pull(this);
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
            isPulling = false;
            ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.receive(this, connection, packet));  // TODO: We do this here because we don't tell the connection who we are when it receives
            connection.receive(packet);
        }

        @Override
        public void pull(@Nullable Receiver<PACKET> receiver) {
            assert receiver == null;
            ResolutionTracer.getIfEnabled().ifPresent(tracer -> tracer.pull(connection, this));  // TODO: Highlights a smell that the connection is pulling and so receiver is null
            if (!isPulling) {
                isPulling = true;
                publishers.forEach(p -> p.pull(this));
            }
        }
    }

    public static abstract class Request<
            PUB_CID, PUB_PROC_ID, PUB_C extends Controller<?, ?, PACKET, ?, PUB_C>, PACKET,
            PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
            CONTROLLER extends Controller<?, PACKET, ?, PROCESSOR, CONTROLLER>,
            REQ extends Request<PUB_CID, PUB_PROC_ID, PUB_C, PACKET, PROCESSOR, ?, REQ>> {

        private final PUB_CID provControllerId;
        private final Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PUB_PROC_ID provProcessorId;

        protected Request(Driver<PROCESSOR> recProcessor, long recEndpointId, PUB_CID provControllerId,
                          PUB_PROC_ID provProcessorId) {
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.provControllerId = provControllerId;
            this.provProcessorId = provProcessorId;
            this.connectionTransforms = new ArrayList<>();
        }

        public abstract Controller.Builder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, ?> getBuilder(CONTROLLER controller);

        public Driver<PROCESSOR> recProcessor() {
            return recProcessor;
        }

        public PUB_CID pubControllerId() {
            return provControllerId;
        }

        public PUB_PROC_ID pubProcessorId() {
            return provProcessorId;
        }

        public long recEndpointId() {
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
            Request<?, ?, ?, ?, ?, ?, ?> request = (Request<?, ?, ?, ?, ?, ?, ?>) o;
            return recEndpointId == request.recEndpointId &&
                    provControllerId.equals(request.provControllerId) &&
                    recProcessor.equals(request.recProcessor) &&
                    connectionTransforms.equals(request.connectionTransforms) &&
                    provProcessorId.equals(request.provProcessorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(provControllerId, recProcessor, recEndpointId, connectionTransforms, provProcessorId);
        }
    }
}
