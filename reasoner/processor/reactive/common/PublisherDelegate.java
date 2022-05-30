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

package com.vaticle.typedb.core.reasoner.processor.reactive.common;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;

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
        Stream<OUTPUT, MAPPED> newOp = TransformationStream.map(publisher.processor(), function);
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(
            Publisher<OUTPUT> publisher, Function<OUTPUT, FunctionalIterator<MAPPED>> function
    ) {
        Stream<OUTPUT, MAPPED> newOp = TransformationStream.flatMap(publisher.processor(), function);
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public Stream<OUTPUT, OUTPUT> distinct(Publisher<OUTPUT> publisher) {
        Stream<OUTPUT, OUTPUT> newOp = TransformationStream.distinct(publisher.processor());
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public Stream<OUTPUT, OUTPUT> buffer(Publisher<OUTPUT> publisher) {
        Stream<OUTPUT, OUTPUT> newOp = PoolingStream.buffer(publisher.processor());
        publisher.registerSubscriber(newOp);
        return newOp;
    }
}
