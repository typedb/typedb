package com.vaticle.typedb.core.reasoner.reactive.common;

import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;

public class SubscriberDelegate<INPUT> {

    private final Reactive.Subscriber<INPUT> subscriber;
    private final AbstractReactiveBlock.Context context;

    public SubscriberDelegate(Reactive.Subscriber<INPUT> subscriber, AbstractReactiveBlock.Context context) {
        this.subscriber = subscriber;
        this.context = context;
    }

    public void registerPath(Reactive.Publisher<INPUT> publisher) {
        subscriber.reactiveBlock().monitor().execute(actor -> actor.registerPath(subscriber.identifier(),
                                                                                 publisher.identifier()));
    }

    public void traceReceive(Reactive.Publisher<INPUT> publisher, INPUT packet) {
        context.tracer().ifPresent(tracer -> tracer.receive(publisher.identifier(), subscriber.identifier(), packet));
    }

    public void rePullPublisher(Reactive.Publisher<INPUT> publisher) {
        subscriber.reactiveBlock().schedulePullRetry(publisher, subscriber);
    }
}
