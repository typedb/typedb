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

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherDelegate;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

import java.util.function.Function;

public class Source<PACKET> extends AbstractReactive implements Reactive.Publisher<PACKET> {

    private final Operator.Source<PACKET> sourceOperator;
    private final SubscriberRegistry.Single<PACKET> subscriberRegistry;
    private final PublisherDelegate<PACKET> publisherDelegate;

    private Source(AbstractProcessor<?, ?, ?, ?> processor, Operator.Source<PACKET> sourceOperator) {
        super(processor);
        this.sourceOperator = sourceOperator;
        this.subscriberRegistry = new SubscriberRegistry.Single<>();
        this.publisherDelegate = new PublisherDelegate<>(this, processor.context());
        processor().monitor().execute(actor -> actor.registerSource(identifier()));
    }

    public static <OUTPUT> Source<OUTPUT> create(
            AbstractProcessor<?, ?, ?, ?> processor,
            java.util.function.Supplier<FunctionalIterator<OUTPUT>> traversalSuppplier
    ) {
        return new Source<>(processor, new Operator.Supplier<>(traversalSuppplier));
    }

    private Operator.Source<PACKET> operator() {
        return sourceOperator;
    }

    @Override
    public void pull(Subscriber<PACKET> subscriber) {
        publisherDelegate.tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        if (!operator().isExhausted(subscriber)) {
            // TODO: Code duplicated in PoolingStream
            subscriberRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a subscriber, so encapsulate it
            publisherDelegate.monitorCreateAnswers(1);
            publisherDelegate.subscriberReceive(subscriber, operator().next(subscriber));
        } else {
            processor().monitor().execute(actor -> actor.sourceFinished(identifier()));
        }
    }

    private SubscriberRegistry<PACKET> subscriberRegistry() {
        return subscriberRegistry;
    }

    @Override
    public void registerSubscriber(Subscriber<PACKET> subscriber) {
        subscriberRegistry.addSubscriber(subscriber);
        subscriber.registerPublisher(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
    }

    @Override
    public <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function) {
        return publisherDelegate.map(this, function);
    }

    @Override
    public <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function) {
        return publisherDelegate.flatMap(this, function);
    }

    @Override
    public Stream<PACKET, PACKET> distinct() {
        return publisherDelegate.distinct(this);
    }

    @Override
    public Stream<PACKET, PACKET> buffer() {
        return publisherDelegate.buffer(this);
    }

}
