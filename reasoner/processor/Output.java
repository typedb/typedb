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
public class Output<PACKET> implements Reactive.Subscriber<PACKET> {

    private final Identifier<?, PACKET> identifier;
    private final AbstractProcessor<?, PACKET, ?, ?> processor;
    private final SubscriberDelegate<PACKET> subscriberActions;
    private Identifier<PACKET, ?> receivingInput;
    private Publisher<PACKET> publisher;

    public Output(AbstractProcessor<?, PACKET, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor().registerReactive(this);
        this.subscriberActions = new SubscriberDelegate<>(this, processor().context());
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
        subscriberActions.traceReceive(publisher, packet);
        receivingInput.processor().execute(actor -> actor.receive(receivingInput, packet, identifier()));
    }

    public void pull() {
        assert publisher != null;
        processor().context().tracer().ifPresent(tracer -> tracer.pull(receivingInput, identifier()));
        publisher.pull(this);
    }

    @Override
    public void registerPublisher(Publisher<PACKET> publisher) {
        assert this.publisher == null;
        this.publisher = publisher;
        subscriberActions.registerPath(publisher);
    }

    public void setSubscriber(Identifier<PACKET, ?> inputId) {
        assert receivingInput == null;
        receivingInput = inputId;
    }

    @Override
    public String toString() {
        return processor().debugName().get() + ":" + getClass().getSimpleName();
    }
}
