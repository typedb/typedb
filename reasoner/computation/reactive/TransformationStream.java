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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator.Transformer;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.SubscriberRegistry;

import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TransformationStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

    private final Transformer<INPUT, OUTPUT> transformer;

    protected TransformationStream(Processor<?, ?, ?, ?> processor,
                                   Transformer<INPUT, OUTPUT> transformer,
                                   SubscriberRegistry<OUTPUT> subscriberRegistry,
                                   ProviderRegistry<Publisher<INPUT>> providerRegistry) {
        super(processor, subscriberRegistry, providerRegistry);
        this.transformer = transformer;
        registerNewPublishers(transformer.initialise());
    }

    public static <INPUT, OUTPUT> TransformationStream<INPUT, OUTPUT> single(
            Processor<?, ?, ?, ?> processor, Transformer<INPUT, OUTPUT> transformer) {
        return new TransformationStream<>(processor, transformer, new SubscriberRegistry.Single<>(),
                                          new ProviderRegistry.Single<>());
    }

    public static <INPUT, OUTPUT> TransformationStream<INPUT, OUTPUT> fanIn(
            Processor<?, ?, ?, ?> processor, Transformer<INPUT, OUTPUT> transformer) {
        return new TransformationStream<>(processor, transformer, new SubscriberRegistry.Single<>(),
                                          new ProviderRegistry.Multi<>());
    }

    protected Transformer<INPUT, OUTPUT> operator() {
        return transformer;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        providerActions.tracePull(subscriber);
        subscriberRegistry().recordPull(subscriber);
        providerRegistry().nonPulling().forEach(this::propagatePull);
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT input) {
        subscriberActions.traceReceive(publisher, input);
        providerRegistry().recordReceive(publisher);

        Operator.Transformed<OUTPUT, INPUT> outcome = operator().accept(publisher, input);
        registerNewPublishers(outcome.newPublishers());
        providerActions.processEffects(outcome);
        if (outcome.outputs().isEmpty() && subscriberRegistry().anyPulling()) {
            subscriberActions.rePullPublisher(publisher);
        } else {
            // pass on the output, regardless of pulling state
            iterate(subscriberRegistry().subscribers()).forEachRemaining(
                    subscriber -> {
                        subscriberRegistry().setNotPulling(subscriber);
                        iterate(outcome.outputs()).forEachRemaining(output -> providerActions.subscriberReceive(subscriber, output));
                    });
        }
    }

    public void registerNewPublishers(Set<Publisher<INPUT>> newPublishers) {
        newPublishers.forEach(newPublisher -> newPublisher.registerSubscriber(this));
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
        return providerActions.map(this, function);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        return providerActions.flatMap(this, function);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> distinct() {
        return providerActions.distinct(this);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        return providerActions.buffer(this);
    }
}
