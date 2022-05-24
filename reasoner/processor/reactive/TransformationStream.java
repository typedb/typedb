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

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator.Transformer;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

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

    private Transformer<INPUT, OUTPUT> operator() {
        return transformer;
    }

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

        Either<Publisher<INPUT>, Set<OUTPUT>> outcome = operator().accept(publisher, input);
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

    private void registerNewPublishers(Set<Publisher<INPUT>> newPublishers) {
        newPublishers.forEach(newPublisher -> newPublisher.registerSubscriber(this));
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
        return getClass().getSimpleName() + ":" + operator().getClass().getSimpleName();
    }

}
