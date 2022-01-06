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

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.PublisherImpl;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver.Subscriber;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public abstract class Processor<INPUT, OUTPUT, PUB_CID, PROCESSOR extends Processor<INPUT, OUTPUT, PUB_CID, PROCESSOR>> extends Actor<PROCESSOR> {

    private final Driver<? extends Controller<PUB_CID, INPUT, OUTPUT, PROCESSOR, ?>> controller;
    private final Reactive<OUTPUT, OUTPUT> outlet;
    private final Map<Long, InletEndpoint<INPUT>> receivingEndpoints;
    private final Map<Long, OutletEndpoint<OUTPUT>> providingEndpoints;
    private long endpointId;

    protected Processor(Driver<PROCESSOR> driver,
                        Driver<? extends Controller<PUB_CID, INPUT, OUTPUT, PROCESSOR, ?>> controller,
                        String name, Reactive<OUTPUT, OUTPUT> outlet) {
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
        try {
            throw e;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    protected InletEndpoint<INPUT> requestConnection(Driver<PROCESSOR> subProcessor, PUB_CID pubControllerId, INPUT pubProcessorId) {
        InletEndpoint<INPUT> endpoint = createReceivingEndpoint();
        controller.execute(actor -> actor.findProviderForConnection(
                new ConnectionRequest<PUB_CID, INPUT, PROCESSOR>(subProcessor, endpoint.id(), pubControllerId, pubProcessorId)));
        return endpoint;
    }

    protected void acceptConnection(ConnectionBuilder<?, OUTPUT, ?, ?> connectionBuilder) {
        Connection<OUTPUT, ?, PROCESSOR> connection = connectionBuilder.build(driver(), nextEndpointId());
        Subscriber<OUTPUT> transformOp = connection.applyConnectionTransforms(createProvidingEndpoint(connection));
        outlet().publishTo(transformOp);
        connectionBuilder.receiverProcessor().execute(actor -> actor.finaliseConnection(connection));
    }

    protected <PUB_PROCESSOR extends Processor<?, INPUT, ?, PUB_PROCESSOR>> void finaliseConnection(Connection<INPUT, ?, PUB_PROCESSOR> connection) {
        receivingEndpoints.get(connection.subEndpointId()).setReady(connection);
    }

    private long nextEndpointId() {
        endpointId += 1;
        return endpointId;
    }

    protected OutletEndpoint<OUTPUT> createProvidingEndpoint(Connection<OUTPUT, ?, PROCESSOR> connection) {
        OutletEndpoint<OUTPUT> endpoint = new OutletEndpoint<>(connection);
        providingEndpoints.put(endpoint.id(), endpoint);
        return endpoint;
    }

    protected InletEndpoint<INPUT> createReceivingEndpoint() {
        InletEndpoint<INPUT> endpoint = new InletEndpoint<>(nextEndpointId());
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

        public InletEndpoint(long id) {
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
            isPulling = true;
            if (ready) connection.pull();
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
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

        public OutletEndpoint(Connection<PACKET, ?, ?> connection) {
            this.publishers = new HashSet<>();
            this.connection = connection;
            this.isPulling = false;
        }

        public long id() {
            return connection.provEndpointId;
        }

        @Override
        public void subscribeTo(Provider<PACKET> publisher) {
            publishers.add(publisher);
            if (isPulling) publisher.pull(this);
        }

        @Override
        public void receive(@Nullable Provider<PACKET> provider, PACKET packet) {
            isPulling = false;
            connection.receive(packet);
        }

        @Override
        public void pull(@Nullable Receiver<PACKET> receiver) {
            assert receiver == null;
            if (!isPulling) {
                isPulling = true;
                publishers.forEach(p -> p.pull(this));
            }
        }
    }

    private static class Connection<PACKET, PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>, PUB_PROCESSOR extends Processor<?, PACKET, ?, PUB_PROCESSOR>> {

        private final Driver<PROCESSOR> recProcessor;
        private final Driver<PUB_PROCESSOR> provProcessor;
        private final long recEndpointId;
        private final long provEndpointId;
        private final List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms;

        /**
         * Connects a processor outlet (upstream, publishing) to another processor's inlet (downstream, subscribing)
         */
        private Connection(Driver<PROCESSOR> recProcessor, Driver<PUB_PROCESSOR> provProcessor, long recEndpointId, long provEndpointId,
                           List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms) {
            this.recProcessor = recProcessor;
            this.provProcessor = provProcessor;
            this.recEndpointId = recEndpointId;
            this.provEndpointId = provEndpointId;
            this.connectionTransforms = connectionTransforms;
        }

        private void receive(PACKET packet) {
            recProcessor.execute(actor -> actor.endpointReceive(packet, recEndpointId));
        }

        private void pull() {
            provProcessor.execute(actor -> actor.endpointPull(provEndpointId));
        }

        private long subEndpointId() {
            return recEndpointId;
        }

        public Subscriber<PACKET> applyConnectionTransforms(OutletEndpoint<PACKET> upstreamEndpoint) {
            assert upstreamEndpoint.id() == provEndpointId;
            Subscriber<PACKET> op = upstreamEndpoint;
            for (Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>> t : connectionTransforms) {
                op = t.apply(op);
            }
            return op;
        }
    }

    public static class ConnectionRequest<PUB_CID, PACKET, PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>> {

        private final PUB_CID provControllerId;
        private final Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms;
        private final PACKET provProcessorId;

        public ConnectionRequest(Driver<PROCESSOR> recProcessor, long recEndpointId, PUB_CID provControllerId,
                                 PACKET provProcessorId) {
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.provControllerId = provControllerId;
            this.provProcessorId = provProcessorId;
            this.connectionTransforms = new ArrayList<>();
        }

        public <PUB_C extends Controller<?, ?, PACKET, ?, PUB_C>> ConnectionBuilder<PUB_CID, PACKET, ?, PUB_C> createConnectionBuilder(Driver<PUB_C> pubController) {
            return new ConnectionBuilder<>(recProcessor, recEndpointId, pubController, provProcessorId, connectionTransforms);
        }

        public PUB_CID pubControllerId() {
            return provControllerId;
        }

        public PACKET pubProcessorId() {
            return provProcessorId;
        }
    }

    public static class ConnectionBuilder<PUB_CID, PACKET, PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
            PUB_CONTROLLER extends Controller<?, ?, PACKET, ?, PUB_CONTROLLER>> {

        private final Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms;
        private final Driver<PUB_CONTROLLER> provController;
        private PACKET provProcessorId;

        protected ConnectionBuilder(Driver<PROCESSOR> recProcessor, long recEndpointId,
                                    Driver<PUB_CONTROLLER> provController,
                                    PACKET provProcessorId,
                                    List<Function<Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms) {
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.provController = provController;
            this.provProcessorId = provProcessorId;
            this.connectionTransforms = connectionTransforms;
        }

        public Driver<PUB_CONTROLLER> providerController() {
            return provController;
        }

        public Driver<PROCESSOR> receiverProcessor() {
            return recProcessor;
        }

        public PACKET receiverProcessorId() {
            return provProcessorId;
        }

        public ConnectionBuilder<PUB_CID, PACKET, PROCESSOR, PUB_CONTROLLER> withMap(Function<PACKET, PACKET> function) {
            connectionTransforms.add(r -> {
                Reactive<PACKET, PACKET> op = noOp();
                op.map(function).publishTo(r);
                return op;
            });
            return this;
        }

        public ConnectionBuilder<PUB_CID, PACKET, PROCESSOR, PUB_CONTROLLER> withNewProcessorId(PACKET newPID) {
            provProcessorId = newPID;
            return this;
        }

        public <PUB_PROCESSOR extends Processor<?, PACKET, ?, PUB_PROCESSOR>> Connection<PACKET, PROCESSOR, PUB_PROCESSOR> build(Driver<PUB_PROCESSOR> pubProcessor, long pubEndpointId) {
            return new Connection<>(recProcessor, pubProcessor, recEndpointId, pubEndpointId, connectionTransforms);
        }
    }

}
