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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Subscriber;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveActions.ProviderActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveActions.ReceiverActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveActions.StreamActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator.Source;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator.Transformer;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class AbstractStream<INPUT, OUTPUT> extends ReactiveImpl implements Publisher<OUTPUT>, Subscriber<INPUT> {  // TODO: Rename Stream when there's no conflict

    private final ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry;
    private final ProviderRegistry<Publisher<INPUT>> providerRegistry;
    protected final ReceiverActions<Publisher<INPUT>, INPUT> receiverActions;
    protected final ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions;
    protected final StreamActions<Publisher<INPUT>> streamActions;

    protected AbstractStream(Processor<?, ?, ?, ?> processor, Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator,
                             ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry, ProviderRegistry<Publisher<INPUT>> providerRegistry,
                             ReceiverActions<Publisher<INPUT>, INPUT> receiverActions,
                             ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions, StreamActions<Publisher<INPUT>> streamActions) {
        super(processor);
        this.receiverRegistry = receiverRegistry;
        this.providerRegistry = providerRegistry;
        this.receiverActions = receiverActions;
        this.providerActions = providerActions;
        this.streamActions = streamActions;
//        if (operator().isSource()) registerSource();  // TODO: Call unconditionally in the constructor of a Source
    }

    private void registerSource() {
        processor().monitor().execute(actor -> actor.registerSource(identifier()));
    }

    public ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry() { return receiverRegistry; }

    public ProviderRegistry<Publisher<INPUT>> providerRegistry() {
        return providerRegistry;
    }

//    public static class ReactiveBuilder {
//
//        private boolean fanOut = false;
//        private boolean fanIn = false;
//        private boolean asyncProvider = false;
//        private boolean asyncReceiver = false;
////        private Transformer<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> transformer;
////        private Operator.Withdrawable<?, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator;
//
//        public ReactiveBuilder fanOut() {
//            fanOut = true;
//            return this;
//        }
//
//        public <PACKET, Subscriber<OUTPUT>> SourceStream<PACKET, Subscriber<OUTPUT>> build(Operator.Source<PACKET, Subscriber<OUTPUT>> supplierOperator) {
//            return new SourceStream<>()
//        }
//
//    }

    public static class TransformationStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

        private final Transformer<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> transformer;

        protected TransformationStream(Processor<?, ?, ?, ?> processor,
                                       Transformer<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> transformer,
                                       ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                                       ProviderRegistry<Publisher<INPUT>> providerRegistry,
                                       ReceiverActions<Publisher<INPUT>, INPUT> receiverActions,
                                       ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions,
                                       StreamActions<Publisher<INPUT>> streamActions) {
            super(processor, transformer, receiverRegistry, providerRegistry, receiverActions, providerActions, streamActions);
            this.transformer = transformer;
        }

        public static <INPUT, OUTPUT> TransformationStream<INPUT, OUTPUT> sync(
                Processor<?, ?, ?, ?> processor, Transformer<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> transformer) {
            ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry = new ReceiverRegistry.Single<>();
            ProviderRegistry<Publisher<INPUT>> providerRegistry = new ProviderRegistry.Single<>();
            ReceiverActions<Publisher<INPUT>, INPUT> receiverActions = new SyncReceiverActions<>(null);
            ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions = new SyncProviderActions<>(null);
            StreamActions<Publisher<INPUT>> streamActions = new SyncStreamActions<>();
            return new TransformationStream<>(processor, transformer, receiverRegistry, providerRegistry,
                                              receiverActions, providerActions, streamActions);
        }

        protected Transformer<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator() {
            return transformer;
        }

        @Override
        public void pull(Subscriber<OUTPUT> subscriber) {
            providerRegistry().nonPulling().forEach(streamActions::propagatePull);
        }

        @Override
        public void registerReceiver(Subscriber<OUTPUT> subscriber) {

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
        public void receive(Publisher<INPUT> publisher, INPUT input) {
            receiverActions.traceReceive(publisher, input);
            providerRegistry().recordReceive(publisher);

            Operator.Transformed<OUTPUT, Publisher<INPUT>> outcome = operator().accept(publisher, input);
            providerActions.processEffects(outcome);
            if (outcome.outputs().isEmpty() && receiverRegistry().anyPulling()) {
                receiverActions.rePullProvider(publisher);
            } else {
                // pass on the output, regardless of pulling state
                iterate(receiverRegistry().receivers()).forEachRemaining(
                        receiver -> iterate(outcome.outputs()).forEachRemaining(output -> providerActions.outputToReceiver(receiver, output)));
            }
        }

        @Override
        public void registerProvider(Publisher<INPUT> publisher) {

        }
    }

    public static abstract class PoolingStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

        private final Operator.Pool<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> pool;

        protected PoolingStream(Processor<?, ?, ?, ?> processor,
                                Operator.Pool<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> pool,
                                ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                                ProviderRegistry<Publisher<INPUT>> providerRegistry,
                                ReceiverActions<Publisher<INPUT>, INPUT> receiverActions,
                                ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions,
                                StreamActions<Publisher<INPUT>> streamActions) {
            super(processor, pool, receiverRegistry, providerRegistry, receiverActions, providerActions, streamActions);
            this.pool = pool;
        }

        protected Operator.Pool<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator() {
            return pool;
        }

        @Override
        public void pull(Subscriber<OUTPUT> receiver) {
            // TODO: We don't care about the receiver here
            if (operator().hasNext(receiver)) {
                WithdrawableHelper.pull(receiver, operator(), providerActions);
            } else {
                // TODO: for POOLING but not for SOURCE
                providerRegistry().nonPulling().forEach(streamActions::propagatePull);
            }
        }

        @Override
        public void receive(Publisher<INPUT> provider, INPUT packet) {
            receiverActions.traceReceive(provider, packet);
            providerRegistry().recordReceive(provider);

            providerActions.processEffects(operator().accept(provider, packet));
            AtomicBoolean retry = new AtomicBoolean();
            retry.set(false);
            iterate(receiverRegistry().pulling()).forEachRemaining(receiver -> {
                if (operator().hasNext(receiver)) {
                    Operator.Supplied<OUTPUT, Publisher<INPUT>> supplied = operator().next(receiver);
                    providerActions.processEffects(supplied);
                    receiverRegistry().setNotPulling(receiver);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
                    providerActions.outputToReceiver(receiver, supplied.output());
                } else {
                    retry.set(true);
                }
            });
            if (retry.get()) receiverActions.rePullProvider(provider);
        }
    }

    public static class WithdrawableHelper {
        // TODO: Can this go inside the providerActions?
        static <OUTPUT, INPUT> void pull(Subscriber<OUTPUT> receiver, Operator.Withdrawable<?, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator, ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions) {
            providerActions.receiverRegistry().setNotPulling(receiver);  // TODO: This call should always be made when sending to a receiver, so encapsulate it
            Operator.Supplied<OUTPUT, Publisher<INPUT>> supplied = operator.next(receiver);
            providerActions.processEffects(supplied);
            providerActions.outputToReceiver(receiver, supplied.output());  // TODO: If the operator isn't tracking which receivers have seen this packet then it needs to be sent to all receivers. So far this is never the case.
        }
    }

    public static class PublisherHelper {  // TODO: Contents can go into ProviderActions

        public static <OUTPUT, MAPPED> Stream<OUTPUT, MAPPED> map(
                Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher, Function<OUTPUT, MAPPED> function) {
            return null;
        }

        public static <OUTPUT, MAPPED> Stream<OUTPUT, MAPPED> flatMap(Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher,
                                                       Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            return null;
        }

        public static <OUTPUT> Stream<OUTPUT, OUTPUT> buffer(Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher) {
            return null;
        }

        public static <OUTPUT> Stream<OUTPUT,OUTPUT> deduplicate(Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher) {
            return null;
        }
    }

    public static class SourceStream<PACKET> extends ReactiveImpl implements Publisher<PACKET> {  // TODO: Rename to Source when there's no clash

        private final Source<PACKET, Subscriber<PACKET>> supplierOperator;
        private final ReceiverRegistry.Single<Subscriber<PACKET>> receiverRegistry;
        private final ProviderActions<Subscriber<PACKET>, PACKET> providerActions;

        protected SourceStream(Processor<?, ?, ?, ?> processor, Source<PACKET, Subscriber<PACKET>> supplierOperator,
                               ReceiverRegistry.Single<Subscriber<PACKET>> receiverRegistry,
                               ProviderActions<Subscriber<PACKET>, PACKET> providerActions) {
            super(processor);
            this.supplierOperator = supplierOperator;
            this.receiverRegistry = receiverRegistry;
            this.providerActions = providerActions;
        }

        public static <OUTPUT> SourceStream<OUTPUT> create(
                Processor<?, ?, ?, ?> processor, Source<OUTPUT, Subscriber<OUTPUT>> operator) {
            return new SourceStream<>(processor, operator, new ReceiverRegistry.Single<>(), new SyncProviderActions<>(null));
        }

        private Source<PACKET, Subscriber<PACKET>> operator() {
            return supplierOperator;
        }

        @Override
        public void pull(Subscriber<PACKET> subscriber) {
            if (operator().hasNext(subscriber)) {
                // WithdrawableHelper.pull(subscriber, operator(), providerActions);
            } else {
                // TODO: Send terminated? This is rather than doing so inside the operator.
            }
        }

        public ReceiverRegistry<Subscriber<PACKET>> receiverRegistry() {
            return receiverRegistry;
        }

        @Override
        public void registerReceiver(Subscriber<PACKET> subscriber) {
            receiverRegistry.addReceiver(subscriber);
        }

        @Override
        public <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function) {
            return PublisherHelper.map(processor, this, function);
        }

        @Override
        public <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function) {
            return PublisherHelper.flatMap(processor, this, function);
        }

        @Override
        public Stream<PACKET, PACKET> buffer() {
            return PublisherHelper.buffer(processor, this);
        }

        @Override
        public Stream<PACKET, PACKET> deduplicate() {
            return PublisherHelper.deduplicate(processor, this);
        }

        @Override
        public Identifier<?, ?> identifier() {
            return null;
        }

    }

    public static class SyncStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> implements Stream<INPUT, OUTPUT> {

        private SyncStream(Processor<?, ?, ?, ?> processor,
                           Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator,
                           ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                           ProviderRegistry<Publisher<INPUT>> providerRegistry,
                           ReceiverActions<Publisher<INPUT>, INPUT> receiverActions,
                           ProviderActions<Subscriber<OUTPUT>, OUTPUT> providerActions,
                           StreamActions<Publisher<INPUT>> streamActions) {
            super(processor, operator, receiverRegistry, providerRegistry, receiverActions, providerActions, streamActions);
        }

//        public static <INPUT, OUTPUT> SyncStream<INPUT, OUTPUT> simple(Processor<?, ?, ?, ?> processor,
//                                                                       Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
//            return new SyncStream<>(processor, operator, new ReceiverRegistry.Single<>(), new ProviderRegistry.Single<>());
//        }

        private void propagatePull(Publisher<INPUT> provider) {
            provider.pull(this);
        }

        @Override
        public void registerReceiver(Subscriber<OUTPUT> subscriber) {
            receiverRegistry().addReceiver(subscriber);
            subscriber.registerProvider(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
        }

        @Override
        public void registerProvider(Publisher<INPUT> publisher) {
            if (providerRegistry().add(publisher)) receiverActions.registerPath(null);  // TODO: Should pass "this"
            if (receiverRegistry().anyPulling() && providerRegistry().setPulling(publisher)) propagatePull(publisher);
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
            return PublisherHelper.map(processor, this, function);
        }

        @Override
        public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            return PublisherHelper.flatMap(processor, this, function);
        }

        @Override
        public Stream<OUTPUT, OUTPUT> buffer() {
            return PublisherHelper.buffer(processor, this);
        }

        @Override
        public Stream<OUTPUT, OUTPUT> deduplicate() {
            return PublisherHelper.deduplicate(processor, this);
        }

        @Override
        public void pull(Subscriber<OUTPUT> outputSubscriber) {

        }

        @Override
        public void receive(Publisher<INPUT> inputPublisher, INPUT input) {

        }
    }

    // TODO: Some of the behaviour of these classes can probably be abstracted
    public static class SyncReceiverActions<INPUT> implements ReceiverActions<Publisher<INPUT>, INPUT> {

        private final Subscriber<INPUT> receiver;
        private final Processor<?, ?, ?, ?> receiverProcessor = null;

        SyncReceiverActions(Subscriber<INPUT> receiver) {
            this.receiver = receiver;
        }

        @Override
        public void registerPath(Publisher<INPUT> publisher) {
            receiverProcessor.monitor().execute(actor -> actor.registerPath(receiver.identifier(), publisher.identifier()));
        }

        @Override
        public void traceReceive(Publisher<INPUT> publisher, INPUT packet) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(publisher.identifier(), receiver.identifier(), packet));
        }

        @Override
        public void rePullProvider(Publisher<INPUT> publisher) {
            receiverProcessor.pullRetry(publisher.identifier(), receiver.identifier());
        }
    }

    public static class SyncProviderActions<OUTPUT> implements ProviderActions<Subscriber<OUTPUT>, OUTPUT> {

        private final Publisher<?> provider;
        private final Processor<?, ?, ?, ?> providerProcessor = null;

        SyncProviderActions(Publisher<?> provider) {
            this.provider = provider;
        }

        @Override
        public void processEffects(Operator.Effects<?> effects) {
            effects.newProviders().forEach(newProvider -> {
                providerProcessor.monitor().execute(actor -> actor.forkFrontier(1, provider.identifier()));
                // newProvider.registerReceiver(this);  // TODO: This is only applicable for Publishers and Subscribers in this case
            });
            for (int i = 0; i <= effects.answersCreated();) {
                // TODO: We can now batch this and even send the delta between created and consumed
                //  in fact we should be able to look at the number of inputs and outputs and move the monitoring
                //  responsibility to streams in a generic way, removing the need for this Outcome object
                providerProcessor.monitor().execute(actor -> actor.createAnswer(provider.identifier()));
            }
            for (int i = 0; i <= effects.answersConsumed();) {
                providerProcessor.monitor().execute(actor -> actor.consumeAnswer(provider.identifier()));
            }
            if (effects.sourceFinished()) providerProcessor.monitor().execute(actor -> actor.sourceFinished(provider.identifier()));
        }

        @Override
        public void outputToReceiver(Subscriber<OUTPUT> subscriber, OUTPUT packet) {
            subscriber.receive(null, packet);  // TODO: Should pass "provider"
        }

        @Override
        public ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry() {
            return null;
        }
    }

    public static class SyncStreamActions<INPUT> implements StreamActions<Publisher<INPUT>> {

        @Override
        public void propagatePull(Publisher<INPUT> publisher) {
            publisher.pull(null);  // TODO should pass "this" but it's unavailable
        }

    }
}
