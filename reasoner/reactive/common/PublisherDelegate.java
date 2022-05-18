package com.vaticle.typedb.core.reasoner.reactive.common;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.reactive.TransformationStream;

import java.util.function.Function;

public class PublisherDelegate<OUTPUT> {

    private final Reactive.Publisher<OUTPUT> publisher;
    private final AbstractReactiveBlock.Context context;

    public PublisherDelegate(Reactive.Publisher<OUTPUT> publisher, AbstractReactiveBlock.Context context) {
        this.publisher = publisher;
        this.context = context;
    }

    public void monitorCreateAnswers(int answersCreated) {
        for (int i = 0; i < answersCreated; i++) {
            publisher.reactiveBlock().monitor().execute(actor -> actor.createAnswer(publisher.identifier()));
        }
    }

    public void monitorConsumeAnswers(int answersConsumed) {
        for (int i = 0; i < answersConsumed; i++) {
            publisher.reactiveBlock().monitor().execute(actor -> actor.consumeAnswer(publisher.identifier()));
        }
    }

    public void subscriberReceive(Reactive.Subscriber<OUTPUT> subscriber, OUTPUT packet) {
        subscriber.receive(publisher, packet);
    }

    public void tracePull(Reactive.Subscriber<OUTPUT> subscriber) {
        context.tracer().ifPresent(tracer -> tracer.pull(subscriber.identifier(), publisher.identifier()));
    }

    public <MAPPED> Reactive.Stream<OUTPUT, MAPPED> map(Reactive.Publisher<OUTPUT> publisher, Function<OUTPUT,
            MAPPED> function) {
        Reactive.Stream<OUTPUT, MAPPED> newOp = TransformationStream.single(publisher.reactiveBlock(),
                                                                            new Operator.Map<>(function));
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public <MAPPED> Reactive.Stream<OUTPUT, MAPPED> flatMap(Reactive.Publisher<OUTPUT> publisher,
                                                            Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        Reactive.Stream<OUTPUT, MAPPED> newOp = TransformationStream.single(publisher.reactiveBlock(),
                                                                            new Operator.FlatMap<>(function));
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public Reactive.Stream<OUTPUT, OUTPUT> distinct(Reactive.Publisher<OUTPUT> publisher) {
        Reactive.Stream<OUTPUT, OUTPUT> newOp = TransformationStream.single(publisher.reactiveBlock(),
                                                                            new Operator.Distinct<>());
        publisher.registerSubscriber(newOp);
        return newOp;
    }

    public Reactive.Stream<OUTPUT, OUTPUT> buffer(Reactive.Publisher<OUTPUT> publisher) {
        Reactive.Stream<OUTPUT, OUTPUT> newOp = PoolingStream.buffer(publisher.reactiveBlock());
        publisher.registerSubscriber(newOp);
        return newOp;
    }
}
