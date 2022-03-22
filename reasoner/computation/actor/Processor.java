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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Sync.Subscriber;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveIdentifier;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.SingleReceiverPublisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class Processor<INPUT, OUTPUT,
        CONTROLLER extends Controller<?, INPUT, OUTPUT, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<INPUT, OUTPUT, ?, PROCESSOR>> extends Actor<PROCESSOR> {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    private final Driver<CONTROLLER> controller;
    private final Map<Reactive.Identifier, InletEndpoint<INPUT>> receivingEndpoints;
    private final Map<Reactive.Identifier, OutletEndpoint<OUTPUT>> providingEndpoints;
    private final Map<Pair<Reactive.Identifier, Reactive.Identifier>, Runnable> pullRetries;
    private final Driver<Monitor> monitor;
    private Reactive.Stream<OUTPUT,OUTPUT> outlet;
    private boolean terminated;
    protected boolean done;
    private long reactiveCounter;

    protected Processor(Driver<PROCESSOR> driver, Driver<CONTROLLER> controller, Driver<Monitor> monitor,
                        Supplier<String> debugName) {
        super(driver, debugName);
        this.controller = controller;
        this.receivingEndpoints = new HashMap<>();
        this.providingEndpoints = new HashMap<>();
        this.done = false;
        this.monitor = monitor;
        this.reactiveCounter = 0;
        this.pullRetries = new HashMap<>();
    }

    public abstract void setUp();

    protected void setOutlet(Reactive.Stream<OUTPUT,OUTPUT> outlet) {
        this.outlet = outlet;
    }

    public void pull() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    public <PACKET> void schedulePullRetry(Provider.Sync<PACKET> provider, Reactive.Receiver.Sync<PACKET> receiver) {
        pullRetries.put(new Pair<>(provider.identifier(), receiver.identifier()), () -> provider.pull(receiver));
        driver().execute(actor -> actor.pullRetry(provider.identifier(), receiver.identifier()));
    }

    public void pullRetry(Reactive.Identifier provider, Reactive.Identifier receiver) {
        pullRetries.get(new Pair<>(provider, receiver)).run();
    }

    public Reactive.Stream<OUTPUT,OUTPUT> outlet() {
        return outlet;
    }

    protected <PROV_CID, PROV_PID, REQ extends Controller.ProviderRequest<PROV_CID, PROV_PID, INPUT, CONTROLLER>> void requestProvider(REQ req) {
        assert !done;
        if (isTerminated()) return;
        controller.execute(actor -> actor.findProviderForRequest(req));
    }

    protected void acceptConnection(ConnectionBuilder<?, OUTPUT> connectionBuilder) {
        assert !done;
        OutletEndpoint<OUTPUT> provider = createProvidingEndpoint();
        provider.setReceiver(connectionBuilder.receiverInputId());
        applyConnectionTransforms(connectionBuilder.connectionTransforms(), outlet(), provider);
        if (isTerminated()) return;
        connectionBuilder.receiverInputId().processor()
                .execute(actor -> actor.finaliseConnection(connectionBuilder.receiverInputId(), provider.identifier()));
    }

    public void applyConnectionTransforms(List<Function<OUTPUT, OUTPUT>> transformations,
                                          Reactive.Stream<OUTPUT,OUTPUT> outlet, OutletEndpoint<OUTPUT> upstreamEndpoint) {
        Provider.Sync.Publisher<OUTPUT> op = outlet;
        for (Function<OUTPUT, OUTPUT> t : transformations) op = op.map(t);
        op.publishTo(upstreamEndpoint);
    }

    protected void finaliseConnection(Reactive.Identifier.Input<INPUT> receiverInputId, Reactive.Identifier.Output<INPUT> providerOutputId) {
        assert !done;
        InletEndpoint<INPUT> inlet = receivingEndpoints.get(receiverInputId);
        inlet.addProvider(providerOutputId);
        inlet.pull();
    }

    protected OutletEndpoint<OUTPUT> createProvidingEndpoint() {
        assert !done;
        OutletEndpoint<OUTPUT> endpoint = new OutletEndpoint<>(this);
        providingEndpoints.put(endpoint.identifier(), endpoint);
        return endpoint;
    }

    protected InletEndpoint<INPUT> createReceivingEndpoint() {
        assert !done;
        InletEndpoint<INPUT> endpoint = new InletEndpoint<>(this);
        receivingEndpoints.put(endpoint.identifier(), endpoint);
        return endpoint;
    }

    public void endpointPull(Reactive.Identifier.Input<OUTPUT> receiver, Reactive.Identifier pubEndpointId) {
        assert !done;
        providingEndpoints.get(pubEndpointId).pull(receiver);
    }

    protected void endpointReceive(Reactive.Identifier.Output<INPUT> provider, INPUT packet, Reactive.Identifier subEndpointId) {
        assert !done;
        receivingEndpoints.get(subEndpointId).receive(provider, packet);
    }

    public Driver<Monitor> monitor() {
        return monitor;
    }

    protected void onFinished(Reactive.Identifier finishable) {
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

    public long incrementReactiveCounter() {
        reactiveCounter += 1;
        return reactiveCounter;
    }

    public Reactive.Identifier registerReactive(Reactive reactive) {
        return new ReactiveIdentifier(driver(), reactive.getClass(), incrementReactiveCounter());
    }

    public Reactive.Identifier.Output<OUTPUT> registerOutlet(OutletEndpoint<OUTPUT> reactive) {
        return new ReactiveIdentifier.Output<>(driver(), reactive.getClass(), incrementReactiveCounter());
    }

    public Reactive.Identifier.Input<INPUT> registerInlet(InletEndpoint<INPUT> reactive) {
        return new ReactiveIdentifier.Input<>(driver(), reactive.getClass(), incrementReactiveCounter());
    }

    /**
     * Governs an input to a processor
     */
    public static class InletEndpoint<PACKET> extends SingleReceiverPublisher<PACKET> implements Reactive.Receiver.Async<PACKET> {

        private final ProviderRegistry.Single<Identifier.Output<PACKET>> providerRegistry;
        private final Identifier.Input<PACKET> identifier;
        private boolean ready;

        public InletEndpoint(Processor<PACKET, ?, ?, ?> processor) {
            super(processor);
            this.ready = false;
            this.providerRegistry = new ProviderRegistry.Single<>(this, processor);
            this.identifier = processor.registerInlet(this);
        }

        private ProviderRegistry.Single<Identifier.Output<PACKET>> providerRegistry() {
            return providerRegistry;
        }

        @Override
        public Identifier.Input<PACKET> identifier() {
            return identifier;
        }

        void addProvider(Reactive.Identifier.Output<PACKET> providerOutputId) {
            providerRegistry().add(providerOutputId);
            assert !ready;
            this.ready = true;
        }

        void pull() {
            pull(receiverRegistry().receiver());
        }

        @Override
        public void pull(Receiver.Sync<PACKET> receiver) {
            assert receiver.equals(receiverRegistry().receiver());
            receiverRegistry().recordPull(receiver);
            if (ready && providerRegistry().setPulling()) {
                providerRegistry().provider().processor()
                        .execute(actor -> actor.endpointPull(identifier(), providerRegistry().provider()));
            }
        }

        @Override
        public void receive(Reactive.Identifier.Output<PACKET> providerId, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(providerId, this, packet));
            providerRegistry().recordReceive(providerId);
            receiverRegistry().setNotPulling();
            receiverRegistry().receiver().receive(this, packet);
        }

    }

    /**
     * Governs an output from a processor
     */
    public static class OutletEndpoint<PACKET> implements Subscriber<PACKET>, Provider.Async<PACKET> {

        private final Reactive.Identifier.Output<PACKET> identifier;
        private final ProviderRegistry.Single<Provider.Sync<PACKET>> providerRegistry;
        private final ReceiverRegistry.SingleReceiverRegistry<Reactive.Identifier.Input<PACKET>> receiverRegistry;

        public OutletEndpoint(Processor<?, PACKET, ?, ?> processor) {
            this.identifier = processor.registerOutlet(this);
            this.providerRegistry = new ProviderRegistry.Single<>(this, processor);
            this.receiverRegistry = new ReceiverRegistry.SingleReceiverRegistry<>();
        }

        @Override
        public Reactive.Identifier.Output<PACKET> identifier() {
            return identifier;
        }

        private ProviderRegistry.Single<Provider.Sync<PACKET>> providerRegistry() {
            return providerRegistry;
        }

        private ReceiverRegistry.SingleReceiverRegistry<Reactive.Identifier.Input<PACKET>> receiverRegistry() {
            return receiverRegistry;
        }

        @Override
        public void receive(Provider.Sync<PACKET> provider, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
            providerRegistry().recordReceive(provider);
            receiverRegistry().setNotPulling();
            receiverRegistry().receiver().processor()
                    .execute(actor -> actor.endpointReceive(identifier(), packet, receiverRegistry().receiver()));
        }

        @Override
        public void pull(Identifier.Input<PACKET> receiverId) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiverRegistry().receiver(), identifier()));
            receiverRegistry().recordPull(receiverId);
            if (providerRegistry().setPulling()) providerRegistry().provider().pull(this);
        }

        @Override
        public void subscribeTo(Provider.Sync<PACKET> provider) {
            providerRegistry().add(provider);
            if (receiverRegistry().isPulling() && providerRegistry().setPulling()) provider.pull(this);
        }

        public void setReceiver(Identifier.Input<PACKET> receiverEndpointId) {
            receiverRegistry().addReceiver(receiverEndpointId);
        }
    }

}