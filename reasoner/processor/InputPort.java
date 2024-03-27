/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherDelegate;

import java.util.function.Function;

/**
 * Governs an input to a processor
 */
public class InputPort<PACKET> implements Reactive.Publisher<PACKET> {

    private final Identifier identifier;
    private final AbstractProcessor<PACKET, ?, ?, ?> processor;
    private final PublisherDelegate<PACKET> publisherDelegate;
    private boolean isReady;
    private Identifier outputPortId;
    private Subscriber<PACKET> subscriber;
    private Actor.Driver<? extends AbstractProcessor<?, PACKET, ?, ?>> outputPortProcessor;

    InputPort(AbstractProcessor<PACKET, ?, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor.registerReactive(this);
        this.isReady = false;
        this.publisherDelegate = new PublisherDelegate<>(this, processor.context());
    }

    @Override
    public AbstractProcessor<?, ?, ?, ?> processor() {
        return processor;
    }

    @Override
    public Identifier identifier() {
        return identifier;
    }

    public void pull() {
        pull(subscriber);
    }

    @Override
    public void pull(Subscriber<PACKET> subscriber) {
        assert subscriber.equals(this.subscriber);
        processor().tracer().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
        if (isReady) outputPortProcessor.execute(actor -> actor.pull(outputPortId));
    }

    public void receive(Identifier outputPortId, PACKET packet) {
        processor().tracer().ifPresent(tracer -> tracer.receive(outputPortId, identifier(), packet));
        subscriber.receive(this, packet);
    }

    @Override
    public void registerSubscriber(Subscriber<PACKET> subscriber) {
        assert this.subscriber == null;
        this.subscriber = subscriber;
        subscriber.registerPublisher(this);
    }

    void setOutputPort(Identifier outputPortId,
                       Actor.Driver<? extends AbstractProcessor<?, PACKET, ?, ?>> outputPortProcessor) {
        assert this.outputPortId == null;
        this.outputPortId = outputPortId;
        this.outputPortProcessor = outputPortProcessor;
        processor().monitor().execute(actor -> actor.registerPath(identifier(), outputPortId));
        assert !isReady;
        isReady = true;
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
    public Stream<PACKET, PACKET> buffer() {
        return publisherDelegate.buffer(this);
    }

    @Override
    public Stream<PACKET, PACKET> distinct() {
        return publisherDelegate.distinct(this);
    }

    @Override
    public String toString() {
        return processor.debugName().get() + ":" + getClass().getSimpleName();
    }

}
