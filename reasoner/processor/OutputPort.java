/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberDelegate;

/**
 * Governs an output from a processor
 */
public class OutputPort<PACKET> implements Reactive.Subscriber<PACKET> {

    private final Identifier identifier;
    private final AbstractProcessor<?, PACKET, ?, ?> processor;
    private final SubscriberDelegate<PACKET> subscriberDelegate;
    private Identifier inputPortId;
    private Publisher<PACKET> publisher;
    private Actor.Driver<? extends AbstractProcessor<PACKET, ?, ?, ?>> inputPortProcessor;

    OutputPort(AbstractProcessor<?, PACKET, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor().registerReactive(this);
        this.subscriberDelegate = new SubscriberDelegate<>(this, processor().context());
    }

    @Override
    public Identifier identifier() {
        return identifier;
    }

    @Override
    public AbstractProcessor<?, PACKET, ?, ?> processor() {
        return processor;
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        subscriberDelegate.traceReceive(publisher, packet);
        inputPortProcessor.execute(actor -> actor.receive(inputPortId, packet, identifier()));
    }

    public void pull() {
        assert publisher != null;
        processor().context().tracer().ifPresent(tracer -> tracer.pull(inputPortId, identifier()));
        publisher.pull(this);
    }

    @Override
    public void registerPublisher(Publisher<PACKET> publisher) {
        assert this.publisher == null;
        this.publisher = publisher;
        subscriberDelegate.registerPath(publisher);
    }

    void setInputPort(
            Identifier inputPortId,
            Actor.Driver<? extends AbstractProcessor<PACKET, ?, ?, ?>> inputPortProcessor
    ) {
        assert this.inputPortId == null;
        this.inputPortId = inputPortId;
        this.inputPortProcessor = inputPortProcessor;
    }

    @Override
    public String toString() {
        return processor().debugName().get() + ":" + getClass().getSimpleName();
    }
}
