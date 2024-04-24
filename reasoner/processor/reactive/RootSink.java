/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberDelegate;

import javax.annotation.Nullable;

public class RootSink<PACKET> implements Reactive.Subscriber.Finishable<PACKET> {

    private final Identifier identifier;
    private final ReasonerConsumer<PACKET> reasonerConsumer;
    private final PublisherRegistry.Single<PACKET> publisherRegistry;
    private final AbstractProcessor<?, PACKET, ?, ?> processor;
    private final SubscriberDelegate<PACKET> subscriberDelegate;
    private boolean isPulling;

    public RootSink(AbstractProcessor<?, PACKET, ?, ?> processor, ReasonerConsumer<PACKET> reasonerConsumer) {
        this.publisherRegistry = new PublisherRegistry.Single<>();
        this.processor = processor;
        this.subscriberDelegate = new SubscriberDelegate<>(this, processor.context());
        this.identifier = processor().registerReactive(this);
        this.reasonerConsumer = reasonerConsumer;
        this.isPulling = false;
        this.reasonerConsumer.setRootProcessor(processor().driver());
        processor().monitor().execute(actor -> actor.registerRoot(processor().driver(), identifier()));
    }

    @Override
    public Identifier identifier() {
        return identifier;
    }

    public void pull() {
        isPulling = true;
        if (publisherRegistry().setPulling()) publisherRegistry().publisher().pull(this);
    }

    @Override
    public void receive(@Nullable Publisher<PACKET> publisher, PACKET packet) {
        subscriberDelegate.traceReceive(publisher, packet);
        publisherRegistry().recordReceive(publisher);
        isPulling = false;
        reasonerConsumer.receiveAnswer(packet);
        processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));
    }

    @Override
    public void registerPublisher(Publisher<PACKET> publisher) {
        publisherRegistry().add(publisher);
        subscriberDelegate.registerPath(publisher);
        if (isPulling && publisherRegistry().setPulling()) publisher.pull(this);
    }

    @Override
    public void finished() {
        reasonerConsumer.finish();
    }

    private PublisherRegistry.Single<PACKET> publisherRegistry() {
        return publisherRegistry;
    }

    @Override
    public AbstractProcessor<?, PACKET, ?, ?> processor() {
        return processor;
    }
}
