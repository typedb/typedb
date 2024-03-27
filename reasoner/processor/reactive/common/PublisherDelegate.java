/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive.common;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream.BufferStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream.DistinctStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream.FlatMapStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream.MapStream;

import java.util.function.Function;


public class PublisherDelegate<OUTPUT> {

    private final Publisher<OUTPUT> publisher;
    private final AbstractProcessor.Context context;

    public PublisherDelegate(Publisher<OUTPUT> publisher, AbstractProcessor.Context context) {
        this.publisher = publisher;
        this.context = context;
    }

    public void monitorCreateAnswers(int answersCreated) {
        for (int i = 0; i < answersCreated; i++) {
            publisher.processor().monitor().execute(actor -> actor.createAnswer(publisher.identifier()));
        }
    }

    public void monitorConsumeAnswers(int answersConsumed) {
        for (int i = 0; i < answersConsumed; i++) {
            publisher.processor().monitor().execute(actor -> actor.consumeAnswer(publisher.identifier()));
        }
    }

    public void subscriberReceive(Reactive.Subscriber<OUTPUT> subscriber, OUTPUT packet) {
        subscriber.receive(publisher, packet);
    }

    public void tracePull(Reactive.Subscriber<OUTPUT> subscriber) {
        context.tracer().ifPresent(tracer -> tracer.pull(subscriber.identifier(), publisher.identifier()));
    }

    public <MAPPED> Stream<OUTPUT, MAPPED> map(Publisher<OUTPUT> publisher, Function<OUTPUT, MAPPED> function) {
        Stream<OUTPUT, MAPPED> newOp = new MapStream<>(publisher.processor(), function);
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(
            Publisher<OUTPUT> publisher, Function<OUTPUT, FunctionalIterator<MAPPED>> function
    ) {
        Stream<OUTPUT, MAPPED> newOp = new FlatMapStream<>(publisher.processor(), function);
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public Stream<OUTPUT, OUTPUT> distinct(Publisher<OUTPUT> publisher) {
        Stream<OUTPUT, OUTPUT> newOp = new DistinctStream<>(publisher.processor());
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public Stream<OUTPUT, OUTPUT> buffer(Publisher<OUTPUT> publisher) {
        Stream<OUTPUT, OUTPUT> newOp = new BufferStream<>(publisher.processor());
        publisher.registerSubscriber(newOp);
        return newOp;
    }
}
