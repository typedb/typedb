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
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.FlatMapOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.MapOperator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

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
        if (operator().isSource()) registerSource();
    }

    private void registerSource() {
        processor().monitor().execute(actor -> actor.registerSource(identifier()));
    }

    @Override
    public Reactive.Identifier<?, ?> identifier() {
        return identifier;
    }

    protected Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> operator() {
        return operator;
    }

    public Processor<?, ?, ?, ?> processor() {
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
        if (operator().isWithdrawable() && operator().asWithdrawable().hasNext(receiver)) {
            receiverRegistry().setNotPulling(receiver);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
            outputToReceiver(receiver, operator().asWithdrawable().next(receiver));  // TODO: If the operator isn't tracking which receivers have seen this packet then it needs to be sent to all receivers. So far this is never the case.
        } else {
            providerRegistry().nonPulling().forEach(this::propagatePull);
        }
    }

    @Override
    public void receive(PROVIDER provider, INPUT packet) {
        traceReceive(provider, packet);
        providerRegistry().recordReceive(provider);
        assert operator().isAccepter();
        if (operator().isTransformer()) {
            Operator.Transformed<OUTPUT, PROVIDER> outcome = operator().asTransformer().accept(provider, packet);  // TODO: Most of the time we want to immediately process and pass on the output, regardless of pulling state
            processEffects(outcome);
            assert !operator().isWithdrawable();  // Transformers shouldn't be withdrawable as they produce all of their outputs straight away
            if (outcome.outputs().isEmpty() && receiverRegistry().anyPulling()) {
                retryPull(provider);
            } else {
                iterate(receiverRegistry().receivers()).forEachRemaining(
                        receiver -> iterate(outcome.outputs()).forEachRemaining(output -> outputToReceiver(receiver, output)));
            }
        } else {
            processEffects(operator().asAccepter().accept(provider, packet));
            AtomicBoolean retry = new AtomicBoolean();
            retry.set(false);
            if (operator().isWithdrawable()) {
                iterate(receiverRegistry().pulling()).forEachRemaining(receiver -> {
                    if (operator().asWithdrawable().hasNext(receiver)) {
                        OUTPUT output = operator().asWithdrawable().next(receiver);
                        receiverRegistry().setNotPulling(receiver);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
                        outputToReceiver(receiver, output);
                    } else {
                        retry.set(true);
                    }
                });
                if (retry.get()) retryPull(provider);
            }
        }
    }

    private void processEffects(Operator.Effects<PROVIDER> effects) {
        effects.newProviders().forEach(newProvider -> {
            processor().monitor().execute(actor -> actor.forkFrontier(1, identifier()));
//            newProvider.registerReceiver(this);  // TODO: This is only applicable for Publishers and Subscribers in this case
        });
        for (int i = 0; i <= effects.answersCreated();) {
            // TODO: We can now batch this and even send the delta between created and consumed
            //  in fact we should be able to look at the number of inputs and outputs and move the monitoring
            //  responsibility to streams in a generic way, removing the need for this Outcome object
            processor().monitor().execute(actor -> actor.createAnswer(identifier()));
        }
        for (int i = 0; i <= effects.answersConsumed();) {
            processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));
        }
        if (effects.sourceFinished()) processor().monitor().execute(actor -> actor.sourceFinished(identifier()));
    }

    protected abstract void traceReceive(PROVIDER provider, INPUT packet);

    protected abstract void propagatePull(PROVIDER provider);

    protected abstract void retryPull(PROVIDER provider);

    protected abstract void outputToReceiver(RECEIVER receiver, OUTPUT packet);

    protected abstract void registerPath(PROVIDER provider);

    public static class SyncStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT, Subscriber<OUTPUT>, Publisher<INPUT>> implements Stream<INPUT, OUTPUT> {

        private SyncStream(Processor<?, ?, ?, ?> processor,
                           Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator,
                           ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                           ProviderRegistry<Publisher<INPUT>> providerRegistry) {
            super(processor, operator, receiverRegistry, providerRegistry);
        }

        @Override
        protected void traceReceive(Publisher<INPUT> publisher, INPUT packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), identifier(), packet));
        }

        public static <INPUT, OUTPUT> SyncStream<INPUT, OUTPUT> simple(Processor<?, ?, ?, ?> processor,
                                                                       Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
            return new SyncStream<>(processor, operator, new ReceiverRegistry.Single<>(), new ProviderRegistry.Single<>());
        }

        public static <INPUT, OUTPUT> SyncStream<INPUT, OUTPUT> fanOut(Processor<?, ?, ?, ?> processor,
                                                                       Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
            return new SyncStream<>(processor, operator, new ReceiverRegistry.Multi<>(), new ProviderRegistry.Single<>());
        }

        public static <INPUT, OUTPUT> SyncStream<INPUT, OUTPUT> fanIn(Processor<?, ?, ?, ?> processor,
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
        protected void outputToReceiver(Subscriber<OUTPUT> receiver, OUTPUT packet) {
            receiver.receive(this, packet);
        }

        @Override
        public void registerReceiver(Subscriber<OUTPUT> subscriber) {
            receiverRegistry().addReceiver(subscriber);
            subscriber.registerProvider(this);
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
            SyncStream<OUTPUT, MAPPED> map = SyncStream.simple(processor(), new MapOperator<>(function));
            registerReceiver(map);
            return map;
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            SyncStream<OUTPUT, MAPPED> flatMap = SyncStream.simple(processor(), new FlatMapOperator<>(function));
            registerReceiver(flatMap);
            return flatMap;
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
        public void registerProvider(Publisher<INPUT> publisher) {
            if (providerRegistry().add(publisher)) registerPath(publisher);
            if (receiverRegistry().anyPulling() && providerRegistry().setPulling(publisher)) propagatePull(publisher);
        }

        @Override
        protected void registerPath(Publisher<INPUT> provider) {
            processor().monitor().execute(actor -> actor.registerPath(identifier(), provider.identifier()));
        }

    }

    public static class InputStream<PACKET> extends AbstractStream<PACKET, PACKET, Subscriber<PACKET>, Reactive.Identifier<?, PACKET>> implements Publisher<PACKET> {

        protected InputStream(Processor<?, ?, ?, ?> processor, Operator<PACKET, PACKET, Identifier<?,
                PACKET>, Subscriber<PACKET>> operator, ReceiverRegistry<Subscriber<PACKET>> receiverRegistry,
                              ProviderRegistry<Reactive.Identifier<?, PACKET>> providerRegistry) {
            super(processor, operator, receiverRegistry, providerRegistry);
        }

        @Override
        protected void traceReceive(Identifier<?, PACKET> packetIdentifier, PACKET packet) {

        }

        @Override
        protected void propagatePull(Reactive.Identifier<?, PACKET> packetIdentifier) {

        }

        @Override
        protected void retryPull(Reactive.Identifier<?, PACKET> packetIdentifier) {

        }

        @Override
        protected void outputToReceiver(Subscriber<PACKET> packetSubscriber, PACKET packet) {

        }

        @Override
        protected void registerPath(Reactive.Identifier<?, PACKET> packetIdentifier) {

        }

        @Override
        public void registerReceiver(Subscriber<PACKET> packetSubscriber) {

        }

        @Override
        public <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function) {
            return null;
        }

        @Override
        public <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function) {
            return null;
        }

        @Override
        public Stream<PACKET, PACKET> buffer() {
            return null;
        }

        @Override
        public Stream<PACKET, PACKET> deduplicate() {
            return null;
        }

        @Override
        public void registerProvider(Identifier<?, PACKET> packetIdentifier) {

        }
    }

    public static class OutputStream<PACKET> extends AbstractStream<PACKET, PACKET, Reactive.Identifier<PACKET, ?>, Publisher<PACKET>> implements Subscriber<PACKET> {

        protected OutputStream(Processor<?, ?, ?, ?> processor,
                               Operator<PACKET, PACKET, Publisher<PACKET>, Identifier<PACKET, ?>> operator,
                               ReceiverRegistry<Reactive.Identifier<PACKET, ?>> receiverRegistry,
                               ProviderRegistry<Publisher<PACKET>> providerRegistry) {
            super(processor, operator, receiverRegistry, providerRegistry);
        }

        @Override
        protected void traceReceive(Publisher<PACKET> packetPublisher, PACKET packet) {

        }

        @Override
        protected void propagatePull(Publisher<PACKET> packetPublisher) {

        }

        @Override
        protected void retryPull(Publisher<PACKET> packetPublisher) {

        }

        @Override
        protected void outputToReceiver(Reactive.Identifier<PACKET, ?> packetIdentifier, PACKET packet) {

        }

        @Override
        protected void registerPath(Publisher<PACKET> packetPublisher) {

        }

        @Override
        public void registerReceiver(Identifier<PACKET, ?> packetIdentifier) {

        }

        @Override
        public void registerProvider(Publisher<PACKET> packetPublisher) {

        }
    }

}
