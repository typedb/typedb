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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Identifier;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver.Subscriber;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveIdentifier;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.SingleReceiverPublisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class Processor<INPUT, OUTPUT,
        REQ extends Connector.Request<?, ?, INPUT>,
        PROCESSOR extends Processor<INPUT, OUTPUT, REQ, PROCESSOR>> extends Actor<PROCESSOR> {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    private final Driver<? extends Controller<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller;
    private final Map<Identifier<?, ?>, Input<INPUT>> inputs;
    private final Map<Identifier<?, ?>, Output<OUTPUT>> outputs;
    private final Map<Pair<Identifier<?, ?>, Identifier<?, ?>>, Runnable> pullRetries;
    private final Driver<Monitor> monitor;
    private Reactive.Stream<OUTPUT,OUTPUT> outputRouter;
    private boolean terminated;
    protected boolean done;
    private long reactiveCounter;

    protected Processor(Driver<PROCESSOR> driver,
                        Driver<? extends Controller<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller,
                        Driver<Monitor> monitor,
                        Supplier<String> debugName) {
        super(driver, debugName);
        this.controller = controller;
        this.inputs = new HashMap<>();
        this.outputs = new HashMap<>();
        this.done = false;
        this.monitor = monitor;
        this.reactiveCounter = 0;
        this.pullRetries = new HashMap<>();
    }

    public abstract void setUp();

    protected void setOutputRouter(Reactive.Stream<OUTPUT, OUTPUT> outputRouter) {
        this.outputRouter = outputRouter;
    }

    public Reactive.Stream<OUTPUT,OUTPUT> outputRouter() {
        return outputRouter;
    }

    public void rootPull() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    public void pull(Identifier<OUTPUT, ?> receiver, Identifier<?, ?> outputId) {
        assert !done;
        outputs.get(outputId).pull(receiver);
    }

    protected void receive(Identifier<?, INPUT> providerId, INPUT input, Identifier<?, ?> inputId) {
        assert !done;
        inputs.get(inputId).receive(providerId, input);
    }

    public <PACKET> void schedulePullRetry(Publisher<PACKET> provider, Subscriber<PACKET> receiver) {
        pullRetries.put(new Pair<>(provider.identifier(), receiver.identifier()), () -> provider.pull(receiver));
        driver().execute(actor -> actor.pullRetry(provider.identifier(), receiver.identifier()));
    }

    public void pullRetry(Identifier<?, ?> provider, Identifier<?, ?> receiver) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pullRetry(receiver, provider));
        pullRetries.get(new Pair<Identifier<?, ?>, Identifier<?, ?>>(provider, receiver)).run();
    }

    protected void requestConnection(REQ req) {
        assert !done;
        if (isTerminated()) return;
        controller.execute(actor -> actor.resolveController(req));
    }

    protected void establishConnection(Connector<?, OUTPUT> connector) {
        assert !done;
        if (isTerminated()) return;
        Output<OUTPUT> output = createOutput();
        output.setReceiver(connector.inputId());
        connector.connectViaTransforms(outputRouter(), output);
        connector.inputId().processor().execute(
                actor -> actor.finishConnection(connector.inputId(), output.identifier()));
    }

    protected void finishConnection(Identifier<INPUT, ?> inputId, Identifier<?, INPUT> outputId) {
        assert !done;
        Input<INPUT> input = inputs.get(inputId);
        input.addProvider(outputId);
        input.pull();
    }

    protected Input<INPUT> createInput() {
        assert !done;
        Input<INPUT> input = new Input<>(this);
        inputs.put(input.identifier(), input);
        return input;
    }

    protected Output<OUTPUT> createOutput() {
        assert !done;
        Output<OUTPUT> output = new Output<>(this);
        outputs.put(output.identifier(), output);
        return output;
    }

    public Driver<Monitor> monitor() {
        return monitor;
    }

    protected void onFinished(Identifier<?, ?> finishable) {
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

    public Identifier<INPUT, OUTPUT> registerReactive(Reactive reactive) {
        return new ReactiveIdentifier<>(driver(), reactive.getClass(), incrementReactiveCounter());
    }

    /**
     * Governs an input to a processor
     */
    public static class Input<PACKET> extends SingleReceiverPublisher<PACKET> implements Reactive.Receiver.Async<PACKET> {

        private final ProviderRegistry.Single<Identifier<?, PACKET>> providerRegistry;
        private final Identifier<PACKET, ?> identifier;
        private boolean ready;

        public Input(Processor<PACKET, ?, ?, ?> processor) {
            super(processor);
            this.identifier = processor.registerReactive(this);
            this.ready = false;
            this.providerRegistry = new ProviderRegistry.Single<>();
        }

        private ProviderRegistry.Single<Identifier<?, PACKET>> providerRegistry() {
            return providerRegistry;
        }

        @Override
        public Identifier<PACKET, ?> identifier() {
            return identifier;
        }

        void addProvider(Identifier<?, PACKET> providerOutputId) {
            if (providerRegistry().add(providerOutputId)) {
                processor().monitor().execute(actor -> actor.registerPath(identifier(), providerOutputId));
            }
            assert !ready;
            this.ready = true;
        }

        void pull() {
            pull(receiverRegistry().receiver());
        }

        @Override
        public void pull(Subscriber<PACKET> subscriber) {
            assert subscriber.equals(receiverRegistry().receiver());
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
            receiverRegistry().recordPull(subscriber);
            if (ready && providerRegistry().setPulling()) {
                providerRegistry().provider().processor()
                        .execute(actor -> actor.pull(identifier(), providerRegistry().provider()));
            }
        }

        @Override
        public void receive(Identifier<?, PACKET> providerId, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(providerId, identifier(), packet));
            providerRegistry().recordReceive(providerId);
            receiverRegistry().setNotPulling();
            receiverRegistry().receiver().receive(this, packet);
        }

    }

    /**
     * Governs an output from a processor
     */
    public static class Output<PACKET> implements Subscriber<PACKET>, Reactive.Provider.Async<PACKET> {

        private final Identifier<?, PACKET> identifier;
        private final Processor<?, PACKET, ?, ?> processor;
        private final ProviderRegistry.Single<Publisher<PACKET>> providerRegistry;
        private final ReceiverRegistry.Single<Identifier<PACKET, ?>> receiverRegistry;

        public Output(Processor<?, PACKET, ?, ?> processor) {
            this.processor = processor;
            this.identifier = processor().registerReactive(this);
            this.providerRegistry = new ProviderRegistry.Single<>();
            this.receiverRegistry = new ReceiverRegistry.Single<>();
        }

        @Override
        public Identifier<?, PACKET> identifier() {
            return identifier;
        }

        private Processor<?, PACKET, ?, ?> processor() {
            return processor;
        }

        private ProviderRegistry.Single<Publisher<PACKET>> providerRegistry() {
            return providerRegistry;
        }

        private ReceiverRegistry.Single<Identifier<PACKET, ?>> receiverRegistry() {
            return receiverRegistry;
        }

        @Override
        public void receive(Publisher<PACKET> publisher, PACKET packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), identifier(), packet));
            providerRegistry().recordReceive(publisher);
            receiverRegistry().setNotPulling();
            receiverRegistry().receiver().processor()
                    .execute(actor -> actor.receive(identifier(), packet, receiverRegistry().receiver()));
        }

        @Override
        public void pull(Identifier<PACKET, ?> receiverId) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiverRegistry().receiver(), identifier()));
            receiverRegistry().recordPull(receiverId);
            if (providerRegistry().setPulling()) providerRegistry().provider().pull(this);
        }

        @Override
        public void registerPublisher(Publisher<PACKET> provider) {
            if (providerRegistry().add(provider)) {
                processor().monitor().execute(actor -> actor.registerPath(identifier(), provider.identifier()));
            }
            if (receiverRegistry().isPulling() && providerRegistry().setPulling()) provider.pull(this);
        }

        public void setReceiver(Identifier<PACKET, ?> inputId) {
            receiverRegistry().addReceiver(inputId);
        }
    }

}
