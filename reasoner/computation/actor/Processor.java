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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.SingleReceiverPublisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected final Set<Connection<INPUT, ?, ?>> upstreamConnections;
    private final TerminationTracker monitoring;
    protected final Set<Monitor.Reference> monitors;
    private Reactive.Stream<OUTPUT,OUTPUT> outlet;
    private long endpointId;
    private boolean terminated;
    protected boolean done;

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

    protected TerminationTracker createMonitoring() {
        return new TerminationTracker(this);
    }

    public abstract void setUp();

    protected void setOutlet(Reactive.Stream<OUTPUT,OUTPUT> outlet) {
        this.outlet = outlet;
    }

    public Reactive.Stream<OUTPUT,OUTPUT> outlet() {
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
                                          Reactive.Stream<OUTPUT,OUTPUT> outlet, OutletEndpoint<OUTPUT> upstreamEndpoint) {
        Provider.Publisher<OUTPUT> op = outlet;
        for (Function<OUTPUT, OUTPUT> t : transformations) op = op.map(t);
        op.publishTo(upstreamEndpoint);
    }

    protected <PROV_PROCESSOR extends Processor<?, INPUT, ?, PROV_PROCESSOR>> void finaliseConnection(Connection<INPUT, ?, PROV_PROCESSOR> connection) {
        assert !done;
        Set<Monitor.Reference> monitors = upstreamMonitors();
        monitors.forEach(connection::registerWithMonitor);
        receivingEndpoints.get(connection.receiverEndpointId()).setReady(connection, upstreamMonitors());
        upstreamConnections.add(connection);
    }

    protected Set<Monitor.Reference> upstreamMonitors() {
        return this.monitors;
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

    protected void endpointPull(Receiver<OUTPUT> receiver, long pubEndpointId, Set<Monitor.Reference> monitors) {
        assert !done;
        addAndPropagateMonitors(monitors);  // TODO: Consider moving this inside the outlet's pull method
        providingEndpoints.get(pubEndpointId).pull(receiver, monitors);
    }

    protected void endpointReceive(Provider<INPUT> provider, INPUT packet, long subEndpointId) {
        assert !done;
        receivingEndpoints.get(subEndpointId).receive(provider, packet);
    }

    public TerminationTracker monitoring() {
        return monitoring;
    }

    protected Set<Monitor.Reference> monitors() {
        return monitors;
    }

    protected void addAndPropagateMonitors(Set<Monitor.Reference> monitors) {
        assert !done;
        Set<Monitor.Reference> newMonitors = newMonitors(monitors);
        Set<Monitor.Reference> newUpstreamMonitors = newUpstreamMonitors(monitors);
        this.monitors.addAll(newMonitors);
        if (newUpstreamMonitors.size() > 0) {
            upstreamConnections.forEach(connection -> {
                newUpstreamMonitors.forEach(connection::registerWithMonitor);
            });
            newMonitors.forEach(monitor -> monitoring().sendSynchronisationReport(monitor));
        }
    }

    protected Set<Monitor.Reference> newUpstreamMonitors(Set<Monitor.Reference> monitors) {
        Set<Monitor.Reference> newMonitors = new HashSet<>(monitors);
        newMonitors.removeAll(this.monitors);
        return newMonitors;
    }

    protected Set<Monitor.Reference> newMonitors(Set<Monitor.Reference> monitors) {
        Set<Monitor.Reference> newMonitors = new HashSet<>(monitors);
        newMonitors.removeAll(this.monitors);
        return newMonitors;
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
    public static class InletEndpoint<PACKET> extends SingleReceiverPublisher<PACKET> implements Receiver<PACKET> {

        private final long id;
        private final ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry;
        private boolean ready;

        public InletEndpoint(long id, TerminationTracker monitor, String groupName) {
            super(monitor, groupName);
            this.id = id;
            this.ready = false;
            this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(this);
        }

        private ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry() {
            return providerRegistry;
        }

        public long id() {
            return id;
        }

        void setReady(Connection<PACKET, ?, ?> connection, Set<Monitor.Reference> monitors) {
            // TODO: Poorly named, it sets ready and pulls
            providerRegistry().add(connection);
            assert !ready;
            this.ready = true;
            pull(receiverRegistry().receiver(), monitors);
        }

        @Override
        public void pull(Receiver<PACKET> receiver, Set<Monitor.Reference> monitors) {
            assert receiver.equals(receiverRegistry().receiver());
            // TODO: What if after ready this receives a different set of monitors to before?
            if (ready && receiverRegistry().recordPull(receiver, monitors)) providerRegistry().pullAll(monitorsToPropagate(monitors));
        }

        Set<Monitor.Reference> monitorsToPropagate(Set<Monitor.Reference> monitors) {
            Set<Monitor.Reference> toPropagate = new HashSet<>(receiverRegistry().monitors());
            toPropagate.addAll(monitors);
            if (tracker().isMonitor()) toPropagate.add(tracker().asMonitor().getReference());
            return toPropagate;
        }

        @Override
        public void receive(Provider<PACKET> provider, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
            providerRegistry().recordReceive(provider);
            receiverRegistry().recordReceive();
            receiverRegistry().receiver().receive(this, packet);
        }
    }

    /**
     * Governs an output from a processor
     */
    public static class OutletEndpoint<PACKET> implements Subscriber<PACKET>, Provider<PACKET> {

        private final ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry;
        private final ReceiverRegistry.SingleReceiverRegistry<PACKET> receiverRegistry;
        private final long id;
        private final TerminationTracker monitor;
        private final String groupName;

        public OutletEndpoint(Connection<PACKET, ?, ?> connection, TerminationTracker monitor, String groupName) {
            this.monitor = monitor;
            this.groupName = groupName;
            this.id = connection.providerEndpointId();
            this.providerRegistry = new ProviderRegistry.SingleProviderRegistry<>(this);
            this.receiverRegistry = new ReceiverRegistry.SingleReceiverRegistry<>(connection);
        }

        private ProviderRegistry.SingleProviderRegistry<PACKET> providerRegistry() {
            return providerRegistry;
        }

        private ReceiverRegistry.SingleReceiverRegistry<PACKET> receiverRegistry() {
            return receiverRegistry;
        }

        protected TerminationTracker monitor() {
            return monitor;
        }

        public long id() {
            return id;
        }

        @Override
        public String groupName() {
            return groupName;
        }

        @Override
        public void receive(Provider<PACKET> provider, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
            providerRegistry().recordReceive(provider);
            receiverRegistry().recordReceive();
            receiverRegistry().receiver().receive(this, packet);
        }

        @Override
        public void pull(Receiver<PACKET> receiver, Set<Monitor.Reference> monitors) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiverRegistry().receiver(), this, monitors));
            if (receiverRegistry().recordPull(receiver, monitors)) providerRegistry().pullAll(receiverRegistry().monitors());
        }

        @Override
        public void subscribeTo(Provider<PACKET> provider) {
            providerRegistry().add(provider);
            if (receiverRegistry().isPulling()) providerRegistry().pull(provider, receiverRegistry().monitors());
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

    public static class TerminationTracker {

        // These counts are used for synchronisation when a new monitor joins, and for the termination in monitor subclasses
        protected long pathsCount;
        protected long answersCount;
        private final Processor<?, ?, ?, ?> processor;

        TerminationTracker(Processor<?, ?, ?, ?> processor) {
            this.processor = processor;
            this.pathsCount = 0;
            this.answersCount = 0;
        }

        public Monitor asMonitor() {
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
            onChange(CountChange.PathFork, numForks, forker);
            processor().monitors().forEach(m -> reportToMonitor(CountChange.PathFork, numForks, forker, m));
        }

        public void onPathJoin(Reactive joiner) {
            onChange(CountChange.PathJoin, 1, joiner);
            processor().monitors().forEach(m -> reportToMonitor(CountChange.PathJoin, 1, joiner, m));
        }

        public void reportPathJoin(Reactive joiner, Monitor.Reference monitor) {
            reportToMonitor(CountChange.PathJoin, 1, joiner, monitor);
        }

        public void onAnswerCreate(Reactive creator) {
            onChange(CountChange.AnswerCreate, 1, creator);
            processor().monitors().forEach(m -> reportToMonitor(CountChange.AnswerCreate, 1, creator, m));
        }

        public void reportAnswerCreate(int num, Reactive creator, Monitor.Reference monitor) {
            assert num >= 0;
            reportToMonitor(CountChange.AnswerCreate, num, creator, monitor);
        }

        public void onAnswerDestroy(Reactive destroyer) {
            onChange(CountChange.AnswerDestroy, 1, destroyer);
            processor().monitors().forEach(m -> reportToMonitor(CountChange.AnswerDestroy, 1, destroyer, m));
        }

        protected void onChange(CountChange countChange, int num, Reactive reactive) {
            updateCount(countChange, num);
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.onCountChange(reactive, countChange, processor().driver(), num));
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

        protected void reportToMonitor(CountChange countChange, int num, Reactive reactive, Monitor.Reference monitor) {
            final int update = num;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.reportCountChange(reactive, countChange, monitor, update));
            monitor.driver().execute(actor -> actor.monitoring().asMonitor().receiveReport(update, countChange));
        }

        protected void sendSynchronisationReport(Monitor.Reference monitor) {
            final long pathsCountUpdate = pathsCount;
            final long answersCountUpdate = answersCount;
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.synchronisationReport(processor().driver(), monitor, pathsCountUpdate, answersCountUpdate));
            monitor.driver().execute(actor -> actor.monitoring().asMonitor().receiveSynchronisationReport(processor().driver(), pathsCountUpdate, answersCountUpdate));
        }

    }

    public static class Monitor extends TerminationTracker {

        private final Set<Driver<? extends Processor<?, ?, ?, ?>>> registered;
        private final Set<Driver<? extends Processor<?, ?, ?, ?>>> countSenders;
        private final Reference reference;
        protected boolean done;

        public Monitor(Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.registered = new HashSet<>();
            this.countSenders = new HashSet<>();
            this.done = false;
            this.reference = new Reference(processor().driver());
        }

        @Override
        public boolean isMonitor() {
            return true;
        }

        @Override
        public Monitor asMonitor() {
            return this;
        }

        public void register(Driver<? extends Processor<?, ?, ?, ?>> registree) {
            registered.add(registree);
        }

        public void receiveSynchronisationReport(Driver<? extends Processor<?, ?, ?, ?>> sender, long pathCountDelta, long answersCountDelta) {
            assert registered.contains(sender);
            assert !countSenders.contains(sender);
            if (!done) {
                pathsCount += pathCountDelta;
                answersCount += answersCountDelta;
                countSenders.add(sender);
                checkDone();
            }
        }

        private void receiveReport(int num, CountChange countChange) {
            if (!done) {
                updateCount(countChange, num);
                checkDone();
            }
        }

        @Override
        protected void updateCount(CountChange countChange, int num) {
            super.updateCount(countChange, num);
            assert answersCount >= 0 || !registered.equals(countSenders);
            assert pathsCount >= -1 || !registered.equals(countSenders);
        }

        @Override
        protected void onChange(CountChange countChange, int num, Reactive reactive) {
            if (!done) {
                super.onChange(countChange, num, reactive);
                checkDone();
            }
        }

        protected void checkDone() {
            assert !done;
            if (registered.equals(countSenders)) {
                assert pathsCount >= -1;
                assert answersCount >= 0;
            }
            if (pathsCount == -1 && answersCount == 0 && processor().isPulling() && registered.equals(countSenders)) {
                done = true;
                processor().onDone();
            }
        }

        public Reference getReference() {
            return reference;
        }

        public static class Reference {

            private final Driver<? extends Processor<?, ?, ?, ?>> driver;

            Reference(Driver<? extends Processor<?, ?, ?, ?>> driver) {
                this.driver = driver;
            }

            public Driver<? extends Processor<?, ?, ?, ?>> driver() {
                return driver;
            }
        }
    }

    public static class NestedMonitor extends Monitor {

        public NestedMonitor(Processor<?, ?, ?, ?> processor) {
            super(processor);
        }

        @Override
        protected void onChange(CountChange countChange, int num, Reactive reactive) {
            if (!done) checkDone();
        }
    }

}
