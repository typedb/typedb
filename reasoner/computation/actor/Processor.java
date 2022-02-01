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
        PROCESSOR extends Processor<INPUT, OUTPUT, ?, PROCESSOR>> extends Actor<PROCESSOR> {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    private final Driver<CONTROLLER> controller;
    private final Map<Long, InletEndpoint<INPUT>> receivingEndpoints;
    private final Map<Long, OutletEndpoint<OUTPUT>> providingEndpoints;
    private final Set<Connection<INPUT, ?, ?>> upstreamConnections;
    private final Monitoring monitoring;
    private final Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors;
    private ReactiveStream<OUTPUT, OUTPUT> outlet;
    private long endpointId;
    private boolean terminated;
    private boolean done;

    protected Processor(Driver<PROCESSOR> driver, Driver<CONTROLLER> controller, String name) {
        super(driver, name);
        this.controller = controller;
        this.endpointId = 0;
        this.receivingEndpoints = new HashMap<>();
        this.providingEndpoints = new HashMap<>();
        this.monitors = new HashSet<>();
        this.upstreamConnections = new HashSet<>();
        this.done = false;
        this.monitoring = createMonitoring();
    }

    protected Monitoring createMonitoring() {
        return new Monitoring(this);
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
        assert !done;
        if (isTerminated()) return;
        controller.execute(actor -> actor.findProviderForConnection(req));
    }

    protected void acceptConnection(Controller.Builder<?, OUTPUT, ?, ?, ?> connectionBuilder) {
        assert !done;
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
        assert !done;
        Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors = addSelfIfMonitor(this.monitors);
        monitors.forEach(connection::registerWithMonitor);
        if (monitors.size() > 0) connection.propagateMonitors(monitors);
        receivingEndpoints.get(connection.receiverEndpointId()).setReady(connection);
        upstreamConnections.add(connection);
    }

    private long nextEndpointId() {
        endpointId += 1;
        return endpointId;
    }

    protected OutletEndpoint<OUTPUT> createProvidingEndpoint(Connection<OUTPUT, ?, PROCESSOR> connection) {
        assert !done;
        OutletEndpoint<OUTPUT> endpoint = new OutletEndpoint<>(connection, monitoring(), name());
        providingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected InletEndpoint<INPUT> createReceivingEndpoint() {
        assert !done;
        InletEndpoint<INPUT> endpoint = new InletEndpoint<>(nextEndpointId(), monitoring(), name());
        receivingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected void endpointPull(long pubEndpointId) {
        assert !done;
        providingEndpoints.get(pubEndpointId).pull(null);
    }

    protected void endpointReceive(INPUT packet, long subEndpointId) {
        assert !done;
        receivingEndpoints.get(subEndpointId).receive(null, packet);
    }

    public Monitoring monitoring() {
        return monitoring;
    }

    protected Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors() {
        return monitors;
    }

    protected void setMonitorReporting(Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors) {
        assert !done;
        Set<Driver<? extends Processor<?, ?, ?, ?>>> unseenMonitors = new HashSet<>(addSelfIfMonitor(monitors));
        unseenMonitors.removeAll(this.monitors);
        this.monitors.addAll(unseenMonitors);
        if (unseenMonitors.size() > 0) {
            upstreamConnections.forEach(connection -> {
                unseenMonitors.forEach(connection::registerWithMonitor);
                connection.propagateMonitors(unseenMonitors);
            });
            unseenMonitors.forEach(monitor -> monitoring().sendInitialReport(monitor));
        }
    }

    private Set<Driver<? extends Processor<?, ?, ?, ?>>> addSelfIfMonitor(Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors) {
        if (monitoring().isMonitor()) {
            Set<Driver<? extends Processor<?, ?, ?, ?>>> newMonitorSet = new HashSet<>(monitors);
            newMonitorSet.add(driver());
            return newMonitorSet;
        } else {
            return monitors;
        }
    }

    protected boolean isPulling() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void onDone() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Processor interrupted by resource close: {}", e.getMessage());
                controller.execute(actor -> actor.exception(e));
                return;
            } else {
                LOG.debug("Processor interrupted by TypeDB exception: {}", e.getMessage());
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

        public InletEndpoint(long id, Monitoring monitor, String groupName) {
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
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(this, connection, monitor().count()));  // TODO: We do this here because we don't tell the connection who we are when we pull
            }
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(connection, this, packet, monitor().count()));  // TODO: Highlights a smell that the connection is receiving and so provider is null
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
        private final Monitoring monitor;
        private final String groupName;

        public OutletEndpoint(Connection<PACKET, ?, ?> connection, Monitoring monitor, String groupName) {
            this.monitor = monitor;
            this.groupName = groupName;
            this.connection = connection;
            this.isPulling = false;
            this.providerManager = new Provider.SingleManager<>(this, monitor());
        }

        protected Monitoring monitor() {
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
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet, monitor().count()));
            isPulling = false;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(this, connection, packet, monitor().count()));  // TODO: We do this here because we don't tell the connection who we are when it receives
            connection.receive(packet);
            providerManager.receivedFrom(provider);
        }

        @Override
        public void pull(@Nullable Receiver<PACKET> receiver) {
            assert receiver == null;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(connection, this, monitor().count()));  // TODO: Highlights a smell that the connection is pulling and so receiver is null
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

    public static class Monitoring {

        protected long pathsCount;
        protected long answersCount;
        private final Processor<?, ?, ?, ?> processor;

        Monitoring(Processor<?, ?, ?, ?> processor) {
            this.processor = processor;
            this.pathsCount = 0;
            this.answersCount = 0;
        }

        protected Monitor asMonitor() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        Processor<?, ?, ?, ?> processor() {
            return processor;
        }

        public boolean isMonitor() {
            return false;
        }

        public enum CountChange {
            PathFork,
            PathJoin,
            AnswerCreate,
            AnswerDestroy
        }

        public void onPathFork(int numForks, Reactive forker) {
            assert numForks > 0;
            onChange(numForks, CountChange.PathFork, forker);
        }

        public void onPathJoin(Reactive joiner) {
            onChange(1, CountChange.PathJoin, joiner);
        }

        public void onAnswerCreate(Reactive creator) {
            onChange(1, CountChange.AnswerCreate, creator);
        }

        public void onAnswerCreate(int num, Reactive creator) {
            assert num >= 0;
            onChange(num, CountChange.AnswerCreate, creator);
        }

        public void onAnswerDestroy(Reactive destroyer) {
            onChange(1, CountChange.AnswerDestroy, destroyer);
        }

        protected void onChange(int num, CountChange countChange, Reactive reactive) {
            updateCount(countChange, num);
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.onCountChange(reactive, countChange, processor().driver(), num));
            reportToMonitors(countChange, num, reactive);
        }

        protected void updateCount(CountChange countChange, int num) {
            switch (countChange) {
                case PathFork:
                    pathsCount += num;
                    break;
                case PathJoin:
                    pathsCount -= num;
                    break;
                case AnswerCreate:
                    answersCount += num;
                    break;
                case AnswerDestroy:
                    answersCount -= num;
                    break;
            }
        }

        protected void reportToMonitors(CountChange countChange, int num, Reactive reactive) {
            final int update = num;
            processor().monitors().forEach(monitor -> {
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.reportCountChange(reactive, countChange, monitor, update));
                monitor.execute(actor -> actor.monitoring().asMonitor().receiveReport(update, countChange, reactive));
            });
        }

        protected void sendInitialReport(Driver<? extends Processor<?, ?, ?, ?>> monitor) {
            final long pathsCountUpdate = pathsCount;
            final long answersCountUpdate = answersCount;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.initialReport(processor().driver(), monitor, pathsCountUpdate, answersCountUpdate));
            monitor.execute(actor -> actor.monitoring().asMonitor().receiveInitialReport(processor().driver(), pathsCountUpdate, answersCountUpdate));
        }

        public long count() {
            return pathsCount + answersCount;
        }

    }

    public static class Monitor extends Monitoring {

        private final Set<Driver<? extends Processor<?, ?, ?, ?>>> registered;
        private final Set<Driver<? extends Processor<?, ?, ?, ?>>> countSenders;

        public Monitor(Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.registered = new HashSet<>();
            this.countSenders = new HashSet<>();
        }

        @Override
        public boolean isMonitor() {
            return true;
        }

        @Override
        protected Monitor asMonitor() {
            return this;
        }

        public void register(Driver<? extends Processor<?, ?, ?, ?>> registree) {
            registered.add(registree);
        }

        public void receiveInitialReport(Driver<? extends Processor<?, ?, ?, ?>> sender, long pathCountDelta, long answersCountDelta) {
            assert registered.contains(sender);
            pathsCount += pathCountDelta;
            answersCount += answersCountDelta;
            countSenders.add(sender);
            checkTermination();
        }

        private void receiveReport(int num, CountChange countChange, Reactive reactive) {
            updateCount(countChange, num);
            reportToMonitors(countChange, num, reactive);
            checkTermination();
        }

        @Override
        protected void updateCount(CountChange countChange, int num) {
            super.updateCount(countChange, num);
            assert answersCount >= 0 || !registered.equals(countSenders);
            assert count() >= -1 || !registered.equals(countSenders);
        }

        @Override
        protected void onChange(int num, CountChange countChange, Reactive reactive) {
            super.onChange(num, countChange, reactive);
            checkTermination();
        }

        private void checkTermination() {
            assert count() >= -1 || !registered.equals(countSenders);
            if (count() <= -1 && processor().isPulling() && registered.equals(countSenders)) {
                processor().onDone();
            }
        }
    }

}
