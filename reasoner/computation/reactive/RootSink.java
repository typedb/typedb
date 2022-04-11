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

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.utils.Tracer.Trace;

import javax.annotation.Nullable;
import java.util.UUID;

public class RootSink implements Reactive.Subscriber.Finishable<ConceptMap>, Reactive.Subscriber<ConceptMap> {

    private final Identifier<?, ?> identifier;
    private final UUID traceId = UUID.randomUUID();
    private final ReasonerConsumer reasonerConsumer;
    private final PublisherRegistry.Single<ConceptMap> publisherRegistry;
    private final Processor<?, ?, ?, ?> processor;
    private final ReactiveImpl.SubscriberActionsImpl<ConceptMap> subscriberActions;
    private boolean isPulling;
    private int traceCounter = 0;

    public RootSink(Processor<ConceptMap, ?, ?, ?> processor, ReasonerConsumer reasonerConsumer) {
        this.publisherRegistry = new PublisherRegistry.Single<>();
        this.processor = processor;
        this.subscriberActions = new ReactiveImpl.SubscriberActionsImpl<>(this);
        this.identifier = processor().registerReactive(this);
        this.reasonerConsumer = reasonerConsumer;
        this.isPulling = false;
        this.reasonerConsumer.initialise(processor().driver());
        processor().monitor().execute(actor -> actor.registerRoot(processor().driver(), identifier()));
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
    public void receive(@Nullable Publisher<ConceptMap> publisher, ConceptMap packet) {
        subscriberActions.traceReceive(publisher, packet);
        publisherRegistry().recordReceive(publisher);
        isPulling = false;
        reasonerConsumer.receiveAnswer(packet);
        processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));
    }

    @Override
    public void registerPublisher(Publisher<ConceptMap> publisher) {
        if (publisherRegistry().add(publisher)) {
            processor().monitor().execute(actor -> actor.registerPath(identifier(), publisher.identifier()));
        }
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
        processor().monitor().execute(actor -> actor.rootFinalised(identifier()));
    }

    protected PublisherRegistry.Single<ConceptMap> publisherRegistry() {
        return publisherRegistry;
    }

    @Override
    public Processor<?, ?, ?, ?> processor() {
        return processor;
    }
}
