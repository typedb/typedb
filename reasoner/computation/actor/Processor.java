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
import com.vaticle.typedb.core.reasoner.computation.reactive.PacketMonitor;
import com.vaticle.typedb.core.reasoner.computation.reactive.PublisherBase;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveStream;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
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

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class Processor<INPUT, OUTPUT,
        CONTROLLER extends Controller<?, INPUT, OUTPUT, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<INPUT, OUTPUT, ?, PROCESSOR>> extends Actor<PROCESSOR> implements PacketMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    private final Driver<CONTROLLER> controller;
    private final Map<Long, InletEndpoint<INPUT>> receivingEndpoints;
    private final Map<Long, OutletEndpoint<OUTPUT>> providingEndpoints;
    protected final Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors;
    private ReactiveStream<OUTPUT, OUTPUT> outlet;
    private long endpointId;
    private boolean terminated;
    private long answerPathsCount;
    private final Set<Connection<INPUT, ?, ?>> upstreamConnections;

    protected Processor(Driver<PROCESSOR> driver, Driver<CONTROLLER> controller, String name) {
        super(driver, name);
        this.controller = controller;
        this.endpointId = 0;
        this.receivingEndpoints = new HashMap<>();
        this.providingEndpoints = new HashMap<>();
        this.monitors = new HashSet<>();
        this.answerPathsCount = 0;
        this.upstreamConnections = new HashSet<>();
    }

    public abstract void setUp();

    protected void setOutlet(ReactiveStream<OUTPUT, OUTPUT> outlet) {
        this.outlet = outlet;
    }

    public ReactiveStream<OUTPUT, OUTPUT> outlet() {
        return outlet;
    }

    protected <PROV_CID, PROV_PROC_ID,
            REQ extends Request<PROV_CID, PROV_PROC_ID, PROV_C, INPUT, PROCESSOR, CONTROLLER, REQ>,
            PROV_C extends Controller<PROV_PROC_ID, ?, INPUT, ?, PROV_C>
            > void requestConnection(REQ req) {
        if (isTerminated()) return;
        controller.execute(actor -> actor.findProviderForConnection(req));
    }

    protected void acceptConnection(Controller.Builder<?, OUTPUT, ?, ?, ?> connectionBuilder) {
        Connection<OUTPUT, ?, PROCESSOR> connection = connectionBuilder.build(driver(), nextEndpointId());
        applyConnectionTransforms(connection.transformations(), outlet(), createProvidingEndpoint(connection));
        if (isTerminated()) return;
        connectionBuilder.receivingProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    public void applyConnectionTransforms(List<Function<OUTPUT, OUTPUT>> transformations,
                                          ReactiveStream<OUTPUT, OUTPUT> outlet, OutletEndpoint<OUTPUT> upstreamEndpoint) {
        Provider.Publisher<OUTPUT> op = outlet;
        for (Function<OUTPUT, OUTPUT> t : transformations) op = op.map(t);
        op.publishTo(upstreamEndpoint);
    }

    protected <PROV_PROCESSOR extends Processor<?, INPUT, ?, PROV_PROCESSOR>> void finaliseConnection(Connection<INPUT, ?, PROV_PROCESSOR> connection) {
        connection.propagateMonitors(addSelfIfMonitor(monitors));
        receivingEndpoints.get(connection.receiverEndpointId()).setReady(connection);
        upstreamConnections.add(connection);
    }

    private long nextEndpointId() {
        endpointId += 1;
        return endpointId;
    }

    protected OutletEndpoint<OUTPUT> createProvidingEndpoint(Connection<OUTPUT, ?, PROCESSOR> connection) {
        OutletEndpoint<OUTPUT> endpoint = new OutletEndpoint<>(connection, this, name());
        providingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected InletEndpoint<INPUT> createReceivingEndpoint() {
        InletEndpoint<INPUT> endpoint = new InletEndpoint<>(nextEndpointId(), this, name());
        receivingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected void endpointPull(long pubEndpointId) {
        providingEndpoints.get(pubEndpointId).pull(null);
    }

    protected void endpointReceive(INPUT packet, long subEndpointId) {
        receivingEndpoints.get(subEndpointId).receive(null, packet);
    }

    protected void addMonitors(Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors) {
        Set<Driver<? extends Processor<?, ?, ?, ?>>> unseenMonitors = new HashSet<>(addSelfIfMonitor(monitors));
        unseenMonitors.removeAll(this.monitors);
        unseenMonitors.forEach(this::fastForwardAnswerPathsCount);
        this.monitors.addAll(unseenMonitors);
        if (unseenMonitors.size() > 0) {
            upstreamConnections.forEach(e -> e.propagateMonitors(unseenMonitors));
        }
    }

    private void fastForwardAnswerPathsCount(Driver<? extends Processor<?, ?, ?, ?>> monitor) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pathCountFastForward(driver(), monitor, answerPathsCount));
        monitor.execute(actor -> actor.updatePathsCount(answerPathsCount));
    }

    public void updatePathsCount(long pathCountDelta) {
        answerPathsCount += pathCountDelta;
        assert answerPathsCount >= -1;
        checkTermination();
    }

    @Override
    public void onPathFork(int numForks, Reactive forker) {
        assert numForks > 0;
        answerPathsCount += numForks;
        assert answerPathsCount >= -1;
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pathFork(forker, driver(), numForks));
        monitors.forEach(monitor -> {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pathFork(forker, monitor, numForks));
            monitor.execute(actor -> actor.onPathFork(numForks, forker));
        });
        checkTermination();
    }

    @Override
    public void onPathJoin(Reactive joiner) {
        answerPathsCount -= 1;
        assert answerPathsCount >= -1;
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pathJoin(joiner, driver(), -1));
        monitors.forEach(monitor -> monitor.execute(actor -> actor.onPathJoin(joiner)));
        checkTermination();
    }

    @Override
    public long pathsCount() {
        return answerPathsCount;
    }

    private Set<Driver<? extends Processor<?, ?, ?, ?>>> addSelfIfMonitor(Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors) {
        if (isMonitor()) {
            Set<Driver<? extends Processor<?, ?, ?, ?>>> newMonitorSet = new HashSet<>(monitors);
            newMonitorSet.add(driver());
            return newMonitorSet;
        } else {
            return monitors;
        }
    }

    private void checkTermination() {
        if (isMonitor()) {
            if (answerPathsCount == -1 && isPulling()) onDone();
        }
    }

    protected boolean isPulling() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void onDone() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected boolean isMonitor() {
        return false;
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

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

    public boolean isTerminated() {
        return terminated;
    }

    /**
     * Governs an input to a processor
     */
    public static class InletEndpoint<PACKET> extends PublisherBase<PACKET> implements Reactive.Receiver<PACKET> {

        private final long id;
        private boolean ready;
        private Connection<PACKET, ?, ?> connection;
        protected boolean isPulling;

        public InletEndpoint(long id, PacketMonitor monitor, String groupName) {
            super(monitor, groupName);
            this.id = id;
            this.ready = false;
            this.isPulling = false;
        }

        public long id() {
            return id;
        }

        void setReady(Connection<PACKET, ?, ?> connection) {
            // TODO: Poorly named, it sets ready and pulls
            this.connection = connection;
            assert !ready;
            this.ready = true;
            pull(subscriber());
        }

        @Override
        public void pull(Receiver<PACKET> receiver) {
            assert receiver.equals(subscriber);
            if (ready && !isPulling) {
                isPulling = true;
                connection.pull();
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, connection, monitor().pathsCount()));  // TODO: We do this here because we don't tell the connection who we are when we pull
            }
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(connection, this, packet, monitor().pathsCount()));  // TODO: Highlights a smell that the connection is receiving and so provider is null
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
        private final SingleManager<PACKET> providerManager;
        protected boolean isPulling;
        private final PacketMonitor monitor;
        private final String groupName;

        public OutletEndpoint(Connection<PACKET, ?, ?> connection, PacketMonitor monitor, String groupName) {
            this.monitor = monitor;
            this.groupName = groupName;
            this.connection = connection;
            this.isPulling = false;
            this.providerManager = new Provider.SingleManager<>(this, monitor());
        }

        protected PacketMonitor monitor() {
            return monitor;
        }

        public long id() {
            return connection.providerEndpointId();
        }

        @Override
        public String groupName() {
            return groupName;
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet, monitor().pathsCount()));
            isPulling = false;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(this, connection, packet, monitor().pathsCount()));  // TODO: We do this here because we don't tell the connection who we are when it receives
            connection.receive(packet);
            providerManager.receivedFrom(provider);
        }

        @Override
        public void pull(@Nullable Receiver<PACKET> receiver) {
            assert receiver == null;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(connection, this, monitor().pathsCount()));  // TODO: Highlights a smell that the connection is pulling and so receiver is null
            if (!isPulling) {
                isPulling = true;
                providerManager.pullAll();
            }
        }

        @Override
        public void subscribeTo(Provider<PACKET> provider) {
            providerManager.add(provider);
            if (isPulling) providerManager.pull(provider);
        }

    }

    public static abstract class Request<
            PROV_CID, PROV_PROC_ID, PROV_C extends Controller<?, ?, PACKET, ?, PROV_C>, PACKET,
            PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
            CONTROLLER extends Controller<?, PACKET, ?, PROCESSOR, CONTROLLER>,
            REQ extends Request<PROV_CID, PROV_PROC_ID, PROV_C, PACKET, PROCESSOR, ?, REQ>> {

        private final PROV_CID provControllerId;
        private final Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PROV_PROC_ID provProcessorId;

        protected Request(Driver<PROCESSOR> recProcessor, long recEndpointId, PROV_CID provControllerId,
                          PROV_PROC_ID provProcessorId) {
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.provControllerId = provControllerId;
            this.provProcessorId = provProcessorId;
            this.connectionTransforms = new ArrayList<>();
        }

        public abstract Controller.Builder<PROV_PROC_ID, PACKET, REQ, PROCESSOR, ?> getBuilder(CONTROLLER controller);

        public Driver<PROCESSOR> receivingProcessor() {
            return recProcessor;
        }

        public PROV_CID providingControllerId() {
            return provControllerId;
        }

        public PROV_PROC_ID providingProcessorId() {
            return provProcessorId;
        }

        public long receivingEndpointId() {
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
