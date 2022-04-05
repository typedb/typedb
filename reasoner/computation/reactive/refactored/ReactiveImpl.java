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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.function.Function;

public abstract class ReactiveImpl implements Reactive {

    protected final Processor<?, ?, ?, ?> processor;
    protected final Reactive.Identifier<?, ?> identifier;

    protected ReactiveImpl(Processor<?, ?, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor().registerReactive(this);
    }

    @Override
    public Reactive.Identifier<?, ?> identifier() {
        return identifier;
    }

    public Processor<?, ?, ?, ?> processor() {
        return processor;
    }

    // TODO: Some of the behaviour of these classes can probably be abstracted
    public static class SubscriberActionsImpl<INPUT> implements ReactiveActions.SubscriberActions<INPUT> {

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

    public static class PublisherActionsImpl<OUTPUT> implements ReactiveActions.PublisherActions<Subscriber<OUTPUT>, OUTPUT> {

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
        }

        @Override
        public void outputToReceiver(Subscriber<OUTPUT> subscriber, OUTPUT packet) {
            subscriber.receive(null, packet);  // TODO: Should pass "provider"
        }

        @Override
        public ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry() {
            return null;
        }

        public <OUTPUT, MAPPED> Stream<OUTPUT, MAPPED> map(
                Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher, Function<OUTPUT, MAPPED> function) {
            return null;
        }

        public <OUTPUT, MAPPED> Stream<OUTPUT, MAPPED> flatMap(Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher,
                                                                      Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
            return null;
        }

        public <OUTPUT> Stream<OUTPUT, OUTPUT> buffer(Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher) {
            return null;
        }

        public <OUTPUT> Stream<OUTPUT,OUTPUT> deduplicate(Processor<?, ?, ?, ?> processor, Publisher<OUTPUT> publisher) {
            return null;
        }
    }
}
