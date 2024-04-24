/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherDelegate;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

import java.util.function.Function;

public class Source<PACKET> extends AbstractReactive implements Reactive.Publisher<PACKET> {

    private final SubscriberRegistry.Single<PACKET> subscriberRegistry;
    private final PublisherDelegate<PACKET> publisherDelegate;
    private final java.util.function.Supplier<FunctionalIterator<PACKET>> traversalSuppplier;
    private FunctionalIterator<PACKET> iterator;

    public Source(AbstractProcessor<?, ?, ?, ?> processor,
                   java.util.function.Supplier<FunctionalIterator<PACKET>> traversalSuppplier) {
        super(processor);
        this.traversalSuppplier = traversalSuppplier;
        this.subscriberRegistry = new SubscriberRegistry.Single<>();
        this.publisherDelegate = new PublisherDelegate<>(this, processor.context());
        processor().monitor().execute(actor -> actor.registerSource(identifier()));
    }

    public PACKET next() {
        assert !isExhausted();
        return iterator().next();
    }

    private boolean isExhausted() {
        return !iterator().hasNext();
    }

    private FunctionalIterator<PACKET> iterator() {
        if (iterator == null) iterator = traversalSuppplier.get();
        return iterator;
    }

    @Override
    public void pull(Subscriber<PACKET> subscriber) {
        publisherDelegate.tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        if (!isExhausted()) {
            // TODO: Code duplicated in PoolingStream
            subscriberRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a subscriber, so encapsulate it
            publisherDelegate.monitorCreateAnswers(1);
            publisherDelegate.subscriberReceive(subscriber, next());
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
