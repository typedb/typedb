/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class PoolingStream<PACKET> extends AbstractStream<PACKET, PACKET> {

    private PoolingStream(AbstractProcessor<?, ?, ?, ?> processor,
                          SubscriberRegistry<PACKET> subscriberRegistry,
                          PublisherRegistry<PACKET> publisherRegistry) {
        super(processor, subscriberRegistry, publisherRegistry);
    }

    abstract boolean accept(Publisher<PACKET> publisher, PACKET packet);

    abstract boolean hasNext(Subscriber<PACKET> subscriber);

    abstract PACKET next(Subscriber<PACKET> subscriber);

    @Override
    public void pull(Subscriber<PACKET> subscriber) {
        publisherDelegate().tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        // TODO: We don't care about the subscriber here
        if (hasNext(subscriber)) {
            // TODO: Code duplicated in Source
            subscriberRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a subscriber, so encapsulate it
            publisherDelegate().subscriberReceive(subscriber, next(subscriber));
        } else {
            publisherRegistry().nonPulling().forEachRemaining(this::propagatePull);
        }
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        subscriberDelegate().traceReceive(publisher, packet);
        publisherRegistry().recordReceive(publisher);
        if (accept(publisher, packet)) publisherDelegate().monitorCreateAnswers(1);
        publisherDelegate().monitorConsumeAnswers(1);
        AtomicBoolean retry = new AtomicBoolean();
        retry.set(false);
        iterate(subscriberRegistry().pulling()).forEachRemaining(subscriber -> {
            if (hasNext(subscriber)) {
                subscriberRegistry().setNotPulling(subscriber);  // TODO: This call should always be made when sending to a subscriber, so encapsulate it
                publisherDelegate().subscriberReceive(subscriber, next(subscriber));
            } else {
                retry.set(true);
            }
        });
        if (retry.get()) subscriberDelegate().rePullPublisher(publisher);
    }

    @Override
    public <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function) {
        return publisherDelegate().map(this, function);
    }

    @Override
    public <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function) {
        return publisherDelegate().flatMap(this, function);
    }

    @Override
    public Stream<PACKET, PACKET> distinct() {
        return publisherDelegate().distinct(this);
    }

    @Override
    public Stream<PACKET, PACKET> buffer() {
        return publisherDelegate().buffer(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public static class BufferStream<PACKET> extends PoolingStream<PACKET> {

        private final Stack<PACKET> stack;

        public BufferStream(AbstractProcessor<?, ?, ?, ?> processor) {
            super(processor, new SubscriberRegistry.Single<>(), new PublisherRegistry.Multi<>());
            this.stack = new Stack<>();
        }

        @Override
        public boolean accept(Publisher<PACKET> publisher, PACKET packet) {
            stack.add(packet);
            return true;
        }

        @Override
        public boolean hasNext(Subscriber<PACKET> subscriber) {
            return stack.size() > 0;
        }

        @Override
        public PACKET next(Subscriber<PACKET> subscriber) {
            return stack.pop();
        }

    }

    public static class BufferedFanStream<PACKET> extends PoolingStream<PACKET> {

        private final java.util.Map<Subscriber<PACKET>, Integer> bufferPositions;  // Points to the next item needed
        private final Set<PACKET> bufferSet;
        private final List<PACKET> bufferList;

        private BufferedFanStream(AbstractProcessor<?, ?, ?, ?> processor, PublisherRegistry<PACKET> publisherRegistry) {
            super(processor, new SubscriberRegistry.Multi<>(), publisherRegistry);
            this.bufferSet = new HashSet<>();
            this.bufferList = new ArrayList<>();
            this.bufferPositions = new HashMap<>();
        }

        public static <PACKET> BufferedFanStream<PACKET> fanOut(AbstractProcessor<?, ?, ?, ?> processor) {
            return new BufferedFanStream<>(processor, new PublisherRegistry.Single<>());
        }

        public static <PACKET> BufferedFanStream<PACKET> fanInFanOut(AbstractProcessor<?, ?, ?, ?> processor) {
            return new BufferedFanStream<>(processor, new PublisherRegistry.Multi<>());
        }

        @Override
        public boolean accept(Publisher<PACKET> publisher, PACKET packet) {
            if (bufferSet.add(packet)) {
                bufferList.add(packet);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean hasNext(Subscriber<PACKET> subscriber) {
            bufferPositions.putIfAbsent(subscriber, 0);
            return bufferList.size() > bufferPositions.get(subscriber);
        }

        @Override
        public PACKET next(Subscriber<PACKET> subscriber) {
            Integer pos = bufferPositions.get(subscriber);
            bufferPositions.put(subscriber, pos + 1);
            return bufferList.get(pos);
        }

    }
}
