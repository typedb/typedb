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

import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberDelegate;

/**
 * Governs an output from a processor
 */
public class OutputPort<PACKET> implements Reactive.Subscriber<PACKET> {

    private final Identifier<?, PACKET> identifier;
    private final AbstractProcessor<?, PACKET, ?, ?> processor;
    private final SubscriberDelegate<PACKET> subscriberDelegate;
    private Identifier<PACKET, ?> inputPortId;
    private Publisher<PACKET> publisher;

    public OutputPort(AbstractProcessor<?, PACKET, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor().registerReactive(this);
        this.subscriberDelegate = new SubscriberDelegate<>(this, processor().context());
    }

    @Override
    public Identifier<?, PACKET> identifier() {
        return identifier;
    }

    @Override
    public AbstractProcessor<?, PACKET, ?, ?> processor() {
        return processor;
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        subscriberDelegate.traceReceive(publisher, packet);
        inputPortId.processor().execute(actor -> actor.receive(inputPortId, packet, identifier()));
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

    public void setInputPort(Identifier<PACKET, ?> inputPortId) {
        assert this.inputPortId == null;
        this.inputPortId = inputPortId;
    }

    @Override
    public String toString() {
        return processor().debugName().get() + ":" + getClass().getSimpleName();
    }
}
