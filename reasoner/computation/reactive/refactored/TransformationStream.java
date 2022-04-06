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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator.Transformer;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class TransformationStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> {

    private final Transformer<INPUT, OUTPUT> transformer;

    protected TransformationStream(Processor<?, ?, ?, ?> processor,
                                   Transformer<INPUT, OUTPUT> transformer) {
        super(processor, new ReceiverRegistry.Single<>(), new ProviderRegistry.Single<>());
        this.transformer = transformer;
    }

    public static <INPUT, OUTPUT> TransformationStream<INPUT, OUTPUT> create(
            Processor<?, ?, ?, ?> processor, Transformer<INPUT, OUTPUT> transformer) {
        return new TransformationStream<>(processor, transformer);
    }

    protected Transformer<INPUT, OUTPUT> operator() {
        return transformer;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        providerActions.tracePull(subscriber);
        receiverRegistry().recordPull(subscriber);
        providerRegistry().nonPulling().forEach(this::propagatePull);
    }

    @Override
    public void receive(Publisher<INPUT> publisher, INPUT input) {
        receiverActions.traceReceive(publisher, input);
        providerRegistry().recordReceive(publisher);

        Operator.Transformed<OUTPUT, INPUT> outcome = operator().accept(publisher, input);
        providerActions.processEffects(outcome);
        if (outcome.outputs().isEmpty() && receiverRegistry().anyPulling()) {
            receiverActions.rePullPublisher(publisher);
        } else {
            // pass on the output, regardless of pulling state
            iterate(receiverRegistry().receivers()).forEachRemaining(
                    receiver -> iterate(outcome.outputs()).forEachRemaining(output -> providerActions.subscriberReceive(receiver, output)));
        }
    }

//    @Override
//    public void registerProvider(Publisher<INPUT> publisher) {
//
//    }

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
