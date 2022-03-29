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

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Receiver;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;

import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractStream<INPUT, OUTPUT, RECEIVER, PROVIDER> implements Provider<RECEIVER>, Receiver<PROVIDER, INPUT> {

    private final Processor<?, ?, ?, ?> processor;
    private final Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> operator;
    private final ReceiverRegistry<RECEIVER> receiverRegistry;
    private final ProviderRegistry<PROVIDER> providerRegistry;
    private final Reactive.Identifier<?, ?> identifier;

    protected AbstractStream(Processor<?, ?, ?, ?> processor, Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> operator,
                             ReceiverRegistry<RECEIVER> receiverRegistry, ProviderRegistry<PROVIDER> providerRegistry) {
        this.processor = processor;
        this.operator = operator;
        this.receiverRegistry = receiverRegistry;
        this.providerRegistry = providerRegistry;
        this.identifier = processor().registerReactive(this);
    }

    @Override
    public Reactive.Identifier<?, ?> identifier() {
        return identifier;
    }

    protected Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> operator() {
        return operator;
    }

    protected Processor<?, ?, ?, ?> processor() {
        return processor;
    }

    protected ReceiverRegistry<RECEIVER> receiverRegistry() {
        return receiverRegistry;
    }

    protected ProviderRegistry<PROVIDER> providerRegistry() {
        return providerRegistry;
    }

    @Override
    public void pull(RECEIVER receiver) {
        Optional<OUTPUT> packet = operator().nextPacket(receiver);
        if (packet.isPresent()) {
            sendOutput(receiver, packet.get());  // TODO: If the operator isn't tracking which receivers have seen this packet then it needs to be sent to all receivers. So far this is never the case.
        } else {
            providerRegistry().nonPulling().forEach(this::propagatePull);
        }
    }

    @Override
    public void receive(PROVIDER provider, INPUT packet) {
        operator().receivePacket(provider, packet);
        receiverRegistry().pulling().forEach(receiver -> {
            Optional<OUTPUT> output = operator().nextPacket(receiver);
            if (output.isPresent()) {
                receiverRegistry().setNotPulling(receiver);
                sendOutput(receiver, output.get());
            } else {
                retryPull(provider);
            }
        });
    }

    protected abstract void propagatePull(PROVIDER provider);

    protected abstract void retryPull(PROVIDER provider);

    protected abstract void sendOutput(RECEIVER receiver, OUTPUT packet);

    protected abstract void registerPath(PROVIDER provider);

    interface Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        void receivePacket(PROVIDER provider, INPUT packet);

        Optional<OUTPUT> nextPacket(RECEIVER receiver);
    }

    public static class SyncStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT, Subscriber<OUTPUT>, Publisher<INPUT>> implements Publisher<OUTPUT>, Subscriber<INPUT> {

        private SyncStream(Processor<?, ?, ?, ?> processor,
                           Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator,
                           ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                           ProviderRegistry<Publisher<INPUT>> providerRegistry) {
            super(processor, operator, receiverRegistry, providerRegistry);
        }

        public SyncStream<INPUT, OUTPUT> simple(Processor<?, ?, ?, ?> processor,
                                                Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
            return new SyncStream<>(processor, operator, new ReceiverRegistry.Single<>(), new ProviderRegistry.Single<>());
        }

        public SyncStream<INPUT, OUTPUT> fanOut(Processor<?, ?, ?, ?> processor,
                                                Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
            return new SyncStream<>(processor, operator, new ReceiverRegistry.Multi<>(), new ProviderRegistry.Single<>());
        }

        public SyncStream<INPUT, OUTPUT> fanIn(Processor<?, ?, ?, ?> processor,
                                               Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
            return new SyncStream<>(processor, operator, new ReceiverRegistry.Single<>(), new ProviderRegistry.Multi<>());
        }

        @Override
        protected void propagatePull(Publisher<INPUT> provider) {
            provider.pull(this);
        }

        @Override
        protected void retryPull(Publisher<INPUT> provider) {
            processor().pullRetry(provider.identifier(), identifier());
        }

        @Override
        protected void sendOutput(Subscriber<OUTPUT> receiver, OUTPUT packet) {
            receiver.receive(this, packet);
        }

        @Override
        public void registerSubscriber(Subscriber<OUTPUT> subscriber) {
            receiverRegistry().addReceiver(subscriber);
            subscriber.registerPublisher(this);
        }

        @Override
        public Stream<OUTPUT, OUTPUT> findFirst() {
            return null;
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
            return null;
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            return null;
        }

        @Override
        public Stream<OUTPUT, OUTPUT> buffer() {
            return null;
        }

        @Override
        public Stream<OUTPUT, OUTPUT> deduplicate() {
            return null;
        }

        @Override
        public void registerPublisher(Publisher<INPUT> publisher) {
            if (providerRegistry().add(publisher)) registerPath(publisher);
            if (receiverRegistry().anyPulling() && providerRegistry().setPulling(publisher)) propagatePull(publisher);
        }

        @Override
        protected void registerPath(Publisher<INPUT> provider) {
            processor().monitor().execute(actor -> actor.registerPath(identifier(), provider.identifier()));
        }
    }

    public static class InputStream<PACKET> extends AbstractStream<PACKET, PACKET, Subscriber<PACKET>, Reactive.Identifier<?, PACKET>> {

        protected InputStream(Processor<?, ?, ?, ?> processor, Operator<PACKET, PACKET, Reactive.Identifier<?,
                PACKET>, Subscriber<PACKET>> operator, ReceiverRegistry<Subscriber<PACKET>> receiverRegistry,
                              ProviderRegistry<Reactive.Identifier<?, PACKET>> providerRegistry) {
            super(processor, operator, receiverRegistry, providerRegistry);
        }

        @Override
        protected void propagatePull(Reactive.Identifier<?, PACKET> packetIdentifier) {

        }

        @Override
        protected void retryPull(Reactive.Identifier<?, PACKET> packetIdentifier) {

        }

        @Override
        protected void sendOutput(Subscriber<PACKET> packetSubscriber, PACKET packet) {

        }

        @Override
        protected void registerPath(Reactive.Identifier<?, PACKET> packetIdentifier) {

        }
    }

    public static class OutputStream<PACKET> extends AbstractStream<PACKET, PACKET, Reactive.Identifier<PACKET, ?>, Publisher<PACKET>> {

        protected OutputStream(Processor<?, ?, ?, ?> processor, Operator<PACKET, PACKET, Publisher<PACKET>,
                Reactive.Identifier<PACKET, ?>> operator,
                               ReceiverRegistry<Reactive.Identifier<PACKET, ?>> receiverRegistry,
                               ProviderRegistry<Publisher<PACKET>> providerRegistry) {
            super(processor, operator, receiverRegistry, providerRegistry);
        }

        @Override
        protected void propagatePull(Publisher<PACKET> packetPublisher) {

        }

        @Override
        protected void retryPull(Publisher<PACKET> packetPublisher) {

        }

        @Override
        protected void sendOutput(Reactive.Identifier<PACKET, ?> packetIdentifier, PACKET packet) {

        }

        @Override
        protected void registerPath(Publisher<PACKET> packetPublisher) {

        }
    }

}
