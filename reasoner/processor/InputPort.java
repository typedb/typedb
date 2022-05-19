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

package com.vaticle.typedb.core.reasoner.processor;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherDelegate;

import java.util.function.Function;

/**
 * Governs an input to a processor
 */
public class InputPort<PACKET> implements Reactive.Publisher<PACKET> {

    private final Identifier<PACKET, ?> identifier;
    private final AbstractProcessor<PACKET, ?, ?, ?> processor;
    private final PublisherDelegate<PACKET> publisherDelegate;
    private boolean isReady;
    private Identifier<?, PACKET> outputPortId;
    private Subscriber<PACKET> subscriber;

    public InputPort(AbstractProcessor<PACKET, ?, ?, ?> processor) {
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
    public Identifier<PACKET, ?> identifier() {
        return identifier;
    }

    public void pull() {
        pull(subscriber);
    }

    @Override
    public void pull(Subscriber<PACKET> subscriber) {
        assert subscriber.equals(this.subscriber);
        processor().tracer().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
        if (isReady)
            outputPortId.processor().execute(actor -> actor.pull(outputPortId));  // TODO: Store the processor
        // rather than getting it from the outputPort
    }

    public void receive(Identifier<?, PACKET> outputPortId, PACKET packet) {
        processor().tracer().ifPresent(tracer -> tracer.receive(outputPortId, identifier(), packet));
        subscriber.receive(this, packet);
    }

    @Override
    public void registerSubscriber(Subscriber<PACKET> subscriber) {
        assert this.subscriber == null;
        this.subscriber = subscriber;
        subscriber.registerPublisher(this);
    }

    public void setOutputPort(Identifier<?, PACKET> outputPortId) {
        assert this.outputPortId == null;
        this.outputPortId = outputPortId;
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
