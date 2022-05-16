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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.reasoner.common.Tracer;

/**
 * Governs an output from a reactiveBlock
 */
public class Output<PACKET> implements Reactive.Subscriber<PACKET> {  // TODO: Move into ReactiveBlock

    private final Identifier<?, PACKET> identifier;
    private final AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock;
    private final AbstractReactive.SubscriberActionsImpl<PACKET> subscriberActions;
    private Identifier<PACKET, ?> receivingInput;
    private Publisher<PACKET> publisher;

    public Output(AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock) {
        this.reactiveBlock = reactiveBlock;
        this.identifier = reactiveBlock().registerReactive(this);
        this.subscriberActions = new AbstractReactive.SubscriberActionsImpl<>(this);
    }

    @Override
    public Identifier<?, PACKET> identifier() {
        return identifier;
    }

    @Override
    public AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock() {
        return reactiveBlock;
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        subscriberActions.traceReceive(publisher, packet);
        receivingInput.reactiveBlock().execute(actor -> actor.receive(identifier(), packet, receivingInput));
    }

    public void pull() {
        assert publisher != null;
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receivingInput, identifier()));
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
        return reactiveBlock.debugName().get() + ":" + getClass().getSimpleName();
    }
}
