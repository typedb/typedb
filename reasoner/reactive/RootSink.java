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

import com.vaticle.typedb.core.reasoner.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.utils.Tracer.Trace;

import javax.annotation.Nullable;
import java.util.UUID;

public class RootSink<PACKET> implements Reactive.Subscriber.Finishable<PACKET>, Reactive.Subscriber<PACKET> {

    private final Identifier<?, ?> identifier;
    private final UUID traceId = UUID.randomUUID();
    private final ReasonerConsumer<PACKET> reasonerConsumer;
    private final PublisherRegistry.Single<PACKET> publisherRegistry;
    private final AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock;
    private final AbstractReactive.SubscriberActionsImpl<PACKET> subscriberActions;
    private boolean isPulling;
    private int traceCounter = 0;

    public RootSink(AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock, ReasonerConsumer<PACKET> reasonerConsumer) {
        this.publisherRegistry = new PublisherRegistry.Single<>();
        this.reactiveBlock = reactiveBlock;
        this.subscriberActions = new AbstractReactive.SubscriberActionsImpl<>(this);
        this.identifier = reactiveBlock().registerReactive(this);
        this.reasonerConsumer = reasonerConsumer;
        this.isPulling = false;
        this.reasonerConsumer.initialise(reactiveBlock().driver());
        reactiveBlock().monitor().execute(actor -> actor.registerRoot(reactiveBlock().driver(), identifier()));
    }

    @Override
    public Identifier<?, ?> identifier() {
        return identifier;
    }

    public void pull() {
        isPulling = true;
        if (publisherRegistry().setPulling()) publisherRegistry().publisher().pull(this);
    }

    @Override
    public void receive(@Nullable Publisher<PACKET> publisher, PACKET packet) {
        subscriberActions.traceReceive(publisher, packet);
        publisherRegistry().recordReceive(publisher);
        isPulling = false;
        reasonerConsumer.receiveAnswer(packet);
        reactiveBlock().monitor().execute(actor -> actor.consumeAnswer(identifier()));
    }

    @Override
    public void registerPublisher(Publisher<PACKET> publisher) {
        if (publisherRegistry().add(publisher)) subscriberActions.registerPath(publisher);
        if (isPulling && publisherRegistry().setPulling()) publisher.pull(this);
    }

    public Trace trace() {
        return Trace.create(traceId, traceCounter);
    }

    public void exception(Throwable e) {
        reasonerConsumer.exception(e);
    }

    @Override
    public void finished() {
        reasonerConsumer.finished();
    }

    protected PublisherRegistry.Single<PACKET> publisherRegistry() {
        return publisherRegistry;
    }

    @Override
    public AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock() {
        return reactiveBlock;
    }
}
