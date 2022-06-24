/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class TransformationStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

    protected TransformationStream(AbstractProcessor<?, ?, ?, ?> processor,
                                   SubscriberRegistry<OUTPUT> subscriberRegistry,
                                   PublisherRegistry<INPUT> publisherRegistry) {
        super(processor, subscriberRegistry, publisherRegistry);
    }

    protected abstract Either<Publisher<INPUT>, Set<OUTPUT>> accept(Publisher<INPUT> publisher, INPUT packet);

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        publisherDelegate().tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        publisherRegistry().nonPulling().forEach(this::propagatePull);
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT input) {
        subscriberDelegate().traceReceive(publisher, input);
        publisherRegistry().recordReceive(publisher);

        Either<Publisher<INPUT>, Set<OUTPUT>> outcome = accept(publisher, input);
        Set<OUTPUT> outputs;
        if (outcome.isFirst()) {
            outcome.first().registerSubscriber(this);
            outputs = set();
        } else {
            outputs = outcome.second();
        }
        if (outputs.size() > 1) publisherDelegate().monitorCreateAnswers(outputs.size() - 1);
        else if (outputs.isEmpty()) publisherDelegate().monitorConsumeAnswers(1);

        if (outputs.isEmpty() && subscriberRegistry().anyPulling()) {
            subscriberDelegate().rePullPublisher(publisher);
        } else {
            // pass on the output, regardless of pulling state
            iterate(subscriberRegistry().subscribers()).forEachRemaining(
                    subscriber -> {
                        subscriberRegistry().setNotPulling(subscriber);
                        iterate(outputs).forEachRemaining(output -> publisherDelegate().subscriberReceive(subscriber, output));
                    });
        }
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
        return publisherDelegate().map(this, function);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        return publisherDelegate().flatMap(this, function);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> distinct() {
        return publisherDelegate().distinct(this);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        return publisherDelegate().buffer(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public static class MapStream<INPUT, OUTPUT> extends TransformationStream<INPUT, OUTPUT> {

        private final Function<INPUT, OUTPUT> mappingFunc;

        public MapStream(AbstractProcessor<?, ?, ?, ?> processor, Function<INPUT, OUTPUT> mappingFunc) {
            super(processor, new SubscriberRegistry.Single<>(), new PublisherRegistry.Single<>());
            this.mappingFunc = mappingFunc;
        }

        @Override
        public Either<Publisher<INPUT>, Set<OUTPUT>> accept(Publisher<INPUT> publisher, INPUT packet) {
            return Either.second(set(mappingFunc.apply(packet)));
        }

    }

    public static class FlatMapStream<INPUT, OUTPUT> extends TransformationStream<INPUT, OUTPUT> {

        private final Function<INPUT, FunctionalIterator<OUTPUT>> mappingFunc;

        public FlatMapStream(AbstractProcessor<?, ?, ?, ?> processor, Function<INPUT, FunctionalIterator<OUTPUT>> mappingFunc) {
            super(processor, new SubscriberRegistry.Single<>(), new PublisherRegistry.Single<>());
            this.mappingFunc = mappingFunc;
        }

        @Override
        public Either<Publisher<INPUT>, Set<OUTPUT>> accept(Publisher<INPUT> publisher, INPUT packet) {
            // This can actually create more receive() calls to downstream than the number of pulls it receives. Protect
            // against by manually adding .buffer() after calls to flatMap
            return Either.second(mappingFunc.apply(packet).toSet());
        }

    }

    public static class DistinctStream<PACKET> extends TransformationStream<PACKET, PACKET> {

        private final Set<PACKET> deduplicationSet;

        public DistinctStream(AbstractProcessor<?, ?, ?, ?> processor) {
            super(processor, new SubscriberRegistry.Single<>(), new PublisherRegistry.Single<>());
            this.deduplicationSet = new HashSet<>();
        }

        @Override
        public Either<Publisher<PACKET>, Set<PACKET>> accept(Publisher<PACKET> publisher, PACKET packet) {
            if (deduplicationSet.add(packet)) return Either.second(set(packet));
            else return Either.second(set());
        }
    }
}
