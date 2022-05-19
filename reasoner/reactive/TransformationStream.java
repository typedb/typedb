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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.reactive.common.Operator.Transformer;
import com.vaticle.typedb.core.reasoner.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.reactive.common.SubscriberRegistry;

import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TransformationStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

    private final Transformer<INPUT, OUTPUT> transformer;

    protected TransformationStream(AbstractProcessor<?, ?, ?, ?> processor,
                                   Transformer<INPUT, OUTPUT> transformer,
                                   SubscriberRegistry<OUTPUT> subscriberRegistry,
                                   PublisherRegistry<INPUT> publisherRegistry) {
        super(processor, subscriberRegistry, publisherRegistry);
        this.transformer = transformer;
        registerNewPublishers(transformer.initialNewPublishers());
    }

    public static <INPUT, OUTPUT> TransformationStream<INPUT, OUTPUT> single(
            AbstractProcessor<?, ?, ?, ?> processor, Transformer<INPUT, OUTPUT> transformer) {
        return new TransformationStream<>(processor, transformer, new SubscriberRegistry.Single<>(),
                                          new PublisherRegistry.Single<>());
    }

    public static <INPUT, OUTPUT> TransformationStream<INPUT, OUTPUT> fanIn(
            AbstractProcessor<?, ?, ?, ?> processor, Transformer<INPUT, OUTPUT> transformer) {
        return new TransformationStream<>(processor, transformer, new SubscriberRegistry.Single<>(),
                                          new PublisherRegistry.Multi<>());
    }

    protected Transformer<INPUT, OUTPUT> operator() {
        return transformer;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        publisherActions.tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        publisherRegistry().nonPulling().forEach(this::propagatePull);
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT input) {
        subscriberActions.traceReceive(publisher, input);
        publisherRegistry().recordReceive(publisher);

        Either<Publisher<INPUT>, Set<OUTPUT>> outcome = operator().accept(publisher, input);
        Set<OUTPUT> outputs;
        if (outcome.isFirst()) {
            outcome.first().registerSubscriber(this);
            outputs = set();
        } else {
            outputs = outcome.second();
        }
        if (outputs.size() > 1) publisherActions.monitorCreateAnswers(outputs.size() - 1);
        else if (outputs.isEmpty()) publisherActions.monitorConsumeAnswers(1);

        if (outputs.isEmpty() && subscriberRegistry().anyPulling()) {
            subscriberActions.rePullPublisher(publisher);
        } else {
            // pass on the output, regardless of pulling state
            iterate(subscriberRegistry().subscribers()).forEachRemaining(
                    subscriber -> {
                        subscriberRegistry().setNotPulling(subscriber);
                        iterate(outputs).forEachRemaining(output -> publisherActions.subscriberReceive(subscriber, output));
                    });
        }
    }

    public void registerNewPublishers(Set<Publisher<INPUT>> newPublishers) {
        newPublishers.forEach(newPublisher -> newPublisher.registerSubscriber(this));
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
        return publisherActions.map(this, function);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        return publisherActions.flatMap(this, function);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> distinct() {
        return publisherActions.distinct(this);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        return publisherActions.buffer(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + operator().getClass().getSimpleName();
    }

}
