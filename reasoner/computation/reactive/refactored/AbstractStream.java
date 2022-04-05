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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Subscriber;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.ReactiveActions.PublisherActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.ReactiveActions.StreamActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.ReactiveActions.SubscriberActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator.Transformer;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class AbstractStream<INPUT, OUTPUT> extends ReactiveImpl implements Publisher<OUTPUT>, Subscriber<INPUT> {  // TODO: Rename Stream when there's no conflict

    private final ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry;
    private final ProviderRegistry<Publisher<INPUT>> providerRegistry;
    protected final SubscriberActions<Publisher<INPUT>, INPUT> receiverActions;
    protected final PublisherActions<Subscriber<OUTPUT>, OUTPUT> providerActions;
    protected final StreamActions<Publisher<INPUT>> streamActions;

    protected AbstractStream(Processor<?, ?, ?, ?> processor, Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator,
                             ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry, ProviderRegistry<Publisher<INPUT>> providerRegistry,
                             SubscriberActions<Publisher<INPUT>, INPUT> receiverActions,
                             PublisherActions<Subscriber<OUTPUT>, OUTPUT> providerActions, StreamActions<Publisher<INPUT>> streamActions) {
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

    public static class WithdrawableHelper {
        // TODO: Can this go inside the providerActions?
        static <OUTPUT, INPUT> void pull(Subscriber<OUTPUT> receiver, Operator.Withdrawable<?, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator, PublisherActions<Subscriber<OUTPUT>, OUTPUT> providerActions) {
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

    // TODO: Some of the behaviour of these classes can probably be abstracted
    public static class SubscriberActionsImpl<INPUT> implements SubscriberActions<Publisher<INPUT>, INPUT> {

        private final Subscriber<INPUT> receiver;
        private final Processor<?, ?, ?, ?> receiverProcessor = null;

        SubscriberActionsImpl(Subscriber<INPUT> receiver) {
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

    public static class PublisherActionsImpl<OUTPUT> implements PublisherActions<Subscriber<OUTPUT>, OUTPUT> {

        private final Publisher<?> provider;
        private final Processor<?, ?, ?, ?> providerProcessor = null;

        PublisherActionsImpl(Publisher<?> provider) {
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

    public static class StreamActionsImpl<INPUT> implements StreamActions<Publisher<INPUT>> {

        @Override
        public void propagatePull(Publisher<INPUT> publisher) {
            publisher.pull(null);  // TODO should pass "this" but it's unavailable
        }

    }
}
