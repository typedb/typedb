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
