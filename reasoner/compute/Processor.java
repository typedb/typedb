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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.reactive.IdentityReactive;
import com.vaticle.typedb.core.reasoner.reactive.PublisherImpl;
import com.vaticle.typedb.core.reasoner.reactive.Provider;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.Receiver;
import com.vaticle.typedb.core.reasoner.reactive.Receiver.Subscriber;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.IdentityReactive.noOp;

public abstract class Processor<PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR extends Processor<PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR>> extends Actor<PROCESSOR> {

    private final Driver<? extends Controller<?, ?, PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR, ?>> controller;
    private final Outlet<OUTPUT> outlet;
    private long endpointId;
    private final Map<Long, InletEndpoint<INPUT>> subscribingEndpoints;
    private final Map<Long, OutletEndpoint<OUTPUT>> publishingEndpoints;

    protected Processor(Driver<PROCESSOR> driver,
                        Driver<? extends Controller<?, ?, PUB_PID, PUB_CID, INPUT, OUTPUT, PROCESSOR, ?>> controller,
                        String name, Outlet<OUTPUT> outlet) {
        super(driver, name);
        this.controller = controller;
        this.outlet = outlet;
        this.endpointId = 0;
        this.subscribingEndpoints = new HashMap<>();
        this.publishingEndpoints = new HashMap<>();
    }

    public Outlet<OUTPUT> outlet() {
        return outlet;
    }

    @Override
    protected void exception(Throwable e) {}

    protected InletEndpoint<INPUT> requestConnection(Driver<PROCESSOR> subscriberProcessor, PUB_CID publisherControllerId, PUB_PID publisherProcessorId) {
        InletEndpoint<INPUT> endpoint = createSubscribingEndpoint();
        controller.execute(actor -> actor.findPublisherForConnection(
                new ConnectionRequest<>(subscriberProcessor, endpoint.id(), publisherControllerId, publisherProcessorId)));
        return endpoint;
    }

    protected void acceptConnection(ConnectionBuilder<?, ?, OUTPUT, ?, ?> connectionBuilder) {
        Connection<OUTPUT, ?, PROCESSOR> connection = connectionBuilder.build(driver(), nextEndpointId());
        outlet().publishTo(connection.applyConnectionTransforms(createPublishingEndpoint(connection)));
        connectionBuilder.subscriberProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    protected <PUB_PROCESSOR extends Processor<?, ?, ?, INPUT, PUB_PROCESSOR>> void finaliseConnection(Connection<INPUT, ?, PUB_PROCESSOR> connection) {
        InletEndpoint<INPUT> endpoint = subscribingEndpoints.get(connection.subEndpointId());
        endpoint.setReady(connection);
    }

    private long nextEndpointId() {
        endpointId += 1;
        return endpointId;
    }

    protected OutletEndpoint<OUTPUT> createPublishingEndpoint(Connection<OUTPUT, ?, PROCESSOR> connection) {
        OutletEndpoint<OUTPUT> endpoint = new OutletEndpoint<>(connection);
        publishingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected InletEndpoint<INPUT> createSubscribingEndpoint() {
        InletEndpoint<INPUT> endpoint = new InletEndpoint<>(nextEndpointId());
        subscribingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected void endpointPull(long pubEndpointId) {
        publishingEndpoints.get(pubEndpointId).pull(null);
    }

    protected void endpointReceive(INPUT packet, long subEndpointId) {
        subscribingEndpoints.get(subEndpointId).receive(null, packet);
    }

    /**
     * Governs an input to a processor
     */
    public static class InletEndpoint<PACKET> extends PublisherImpl<PACKET> implements Receiver<PACKET> {

        private final long id;
        private boolean ready;
        private Connection<PACKET, ?, ?> connection;

        public InletEndpoint(long id) {
            this.id = id;
            this.ready = false;
        }

        public long id() {
            return id;
        }

        void setReady(Connection<PACKET, ?, ?> connection) {
            this.connection = connection;
            this.ready = true;
        }

        @Override
        public void pull(Receiver<PACKET> receiver) {
            assert ready;
            subscribers.add(receiver);
            connection.pull();
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            assert provider == null;
            subscribers().forEach(subscriber -> subscriber.receive(this, packet));
        }
    }
    /**
     * Governs an output from a processor
     */
    public static class OutletEndpoint<PACKET> implements Subscriber<PACKET>, Provider<PACKET> {

        private final Connection<PACKET, ?, ?> connection;
        private final Set<Provider<PACKET>> publishers;
        protected boolean isPulling;

        public OutletEndpoint(Connection<PACKET, ?, ?> connection) {
            this.publishers = new HashSet<>();
            this.connection = connection;
        }

        public long id() {
            return connection.pubEndpointId;
        }

        @Override
        public void subscribeTo(Provider<PACKET> publisher) {
            publishers.add(publisher);
            if (isPulling) publisher.pull(this);
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            connection.receive(packet);
        }

        @Override
        public void pull(@Nullable Receiver<PACKET> receiver) {
            assert receiver == null;
            if (!isPulling) {
                publishers.forEach(p -> p.pull(this));
                isPulling = true;
            }
        }
    }

    private static class Connection<PACKET, PROCESSOR extends Processor<?, ?, PACKET, ?, PROCESSOR>, PUB_PROCESSOR extends Processor<?, ?, ?, PACKET, PUB_PROCESSOR>> {

        private final Driver<PROCESSOR> subProcessor;
        private final Driver<PUB_PROCESSOR> pubProcessor;
        private final long subEndpointId;
        private final long pubEndpointId;
        private final List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms;

        /**
         * Connects a processor outlet (upstream, publishing) to another processor's inlet (downstream, subscribing)
         */
        private Connection(Driver<PROCESSOR> subProcessor, Driver<PUB_PROCESSOR> pubProcessor, long subEndpointId, long pubEndpointId,
                           List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms) {
            this.subProcessor = subProcessor;
            this.pubProcessor = pubProcessor;
            this.subEndpointId = subEndpointId;
            this.pubEndpointId = pubEndpointId;
            this.connectionTransforms = connectionTransforms;
        }

        private void receive(PACKET packet) {
            subProcessor.execute(actor -> actor.endpointReceive(packet, subEndpointId));
        }

        private void pull() {
            pubProcessor.execute(actor -> actor.endpointPull(pubEndpointId));
        }

        private long subEndpointId() {
            return subEndpointId;
        }

        public Subscriber<PACKET> applyConnectionTransforms(OutletEndpoint<PACKET> upstreamEndpoint) {
            assert upstreamEndpoint.id() == pubEndpointId;
            Subscriber<PACKET> op = upstreamEndpoint;
            for (Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>> t : connectionTransforms) {
                op = t.apply(op);
            }
            return op;
        }
    }

    public static class ConnectionRequest<PUB_CID, PUB_PID, PUB_OUTPUT, PROCESSOR extends Processor<?, ?, PUB_OUTPUT,
            ?, PROCESSOR>> {

        private final PUB_CID pubControllerId;
        private final Driver<PROCESSOR> subProcessor;
        private final long subEndpointId;
        private final List<Function<Subscriber<PUB_OUTPUT>, Reactive<PUB_OUTPUT, PUB_OUTPUT>>> connectionTransforms;
        private final PUB_PID pubProcessorId;

        protected ConnectionRequest(Driver<PROCESSOR> subProcessor, long subEndpointId, PUB_CID pubControllerId, PUB_PID pubProcessorId) {
            this.subProcessor = subProcessor;
            this.subEndpointId = subEndpointId;
            this.pubControllerId = pubControllerId;
            this.pubProcessorId = pubProcessorId;
            this.connectionTransforms = new ArrayList<>();
        }

        public <PUB_C extends Controller<?, PUB_PID, ?, ?, ?, PUB_OUTPUT, ?, PUB_C>> ConnectionBuilder<PUB_CID,
                        PUB_PID, PUB_OUTPUT, PROCESSOR, PUB_C> createConnectionBuilder(Driver<PUB_C> pubController) {
            return new ConnectionBuilder<>(subProcessor, subEndpointId, pubController, pubProcessorId, connectionTransforms);
        }

        public PUB_CID pubControllerId() {
            return pubControllerId;
        }

        public PUB_PID pubProcessorId() {
            return pubProcessorId;
        }
    }

    public static class ConnectionBuilder<PUB_CID, PUB_PID, PUB_OUTPUT, PROCESSOR extends Processor<?, ?, PUB_OUTPUT,
            ?, PROCESSOR>, PUB_CONTROLLER extends Controller<?, PUB_PID, ?, ?, ?, PUB_OUTPUT, ?, PUB_CONTROLLER>> {

        private final Driver<PROCESSOR> subProcessor;
        private final long subEndpointId;
        private final List<Function<Subscriber<PUB_OUTPUT>, Reactive<PUB_OUTPUT, PUB_OUTPUT>>> connectionTransforms;
        private final Driver<PUB_CONTROLLER> pubController;
        private PUB_PID pubProcessorId;

        protected ConnectionBuilder(Driver<PROCESSOR> subProcessor, long subEndpointId,
                                    Driver<PUB_CONTROLLER> pubController,
                                    PUB_PID pubProcessorId, List<Function<Subscriber<PUB_OUTPUT>, Reactive<PUB_OUTPUT, PUB_OUTPUT>>> connectionTransforms) {
            this.subProcessor = subProcessor;
            this.subEndpointId = subEndpointId;
            this.pubController = pubController;
            this.pubProcessorId = pubProcessorId;
            this.connectionTransforms = connectionTransforms;
        }

        public Driver<PUB_CONTROLLER> publisherController() {
            return pubController;
        }

        public Driver<PROCESSOR> subscriberProcessor() {
            return subProcessor;
        }

        public PUB_PID publisherProcessorId() {
            return pubProcessorId;
        }

        public ConnectionBuilder<PUB_CID, PUB_PID, PUB_OUTPUT, PROCESSOR, PUB_CONTROLLER> withMap(PUB_PID newPID, Function<PUB_OUTPUT, PUB_OUTPUT> function) {
            connectionTransforms.add(r -> {
                Reactive<PUB_OUTPUT, PUB_OUTPUT> op = noOp();
                op.map(function).publishTo(r);
                return op;
            });
            pubProcessorId = newPID;
            return this;
        }

        public <PUB_PROCESSOR extends Processor<?, ?, ?, PUB_OUTPUT, PUB_PROCESSOR>> Connection<PUB_OUTPUT, PROCESSOR, PUB_PROCESSOR> build(Driver<PUB_PROCESSOR> pubProcessor, long pubEndpointId) {
            return new Connection<>(subProcessor, pubProcessor, subEndpointId, pubEndpointId, connectionTransforms);
        }
    }

    public static abstract class Outlet<OUTPUT> extends IdentityReactive<OUTPUT> {

        Outlet() {
            super(set());
        }

        public static class Single<OUTPUT> extends Outlet<OUTPUT> {

            @Override
            public void publishTo(Subscriber<OUTPUT> subscriber) {
                if (subscribers().size() > 0) throw TypeDBException.of(ILLEGAL_STATE);
                else subscribers.add(subscriber);
            }
        }

        public static class DynamicMulti<OUTPUT> extends Outlet<OUTPUT> {

            @Override
            public void publishTo(Subscriber<OUTPUT> subscriber) {
                super.publishTo(subscriber);  // TODO: This needs to record the subscribers read position in the buffer and maintain it.
            }
        }
    }

    public static class Source<PACKET> extends PublisherImpl<PACKET> {

        private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
        private final Map<Receiver<PACKET>, FunctionalIterator<PACKET>> iterators;

        public Source(Supplier<FunctionalIterator<PACKET>> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
            this.iterators = new HashMap<>();
        }

        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier) {
            return new Source<>(iteratorSupplier);
        }

        @Override
        public void pull(Receiver<PACKET> receiver) {
            FunctionalIterator<PACKET> iterator = iterators.computeIfAbsent(receiver, s -> iteratorSupplier.get());
            assert iterators.size() <= 1;  // Opens a new iterator for each subscriber. Presently we only intend to
            // have one subscriber, so look into this if more are required
            if (iterator.hasNext()) receiver.receive(this, iterator.next());
        }

    }
}
