/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive.common;

import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;

public class SubscriberDelegate<INPUT> {

    private final Reactive.Subscriber<INPUT> subscriber;
    private final AbstractProcessor.Context context;

    public SubscriberDelegate(Reactive.Subscriber<INPUT> subscriber, AbstractProcessor.Context context) {
        this.subscriber = subscriber;
        this.context = context;
    }

    public void registerPath(Reactive.Publisher<INPUT> publisher) {
        subscriber.processor().monitor().execute(actor -> actor.registerPath(subscriber.identifier(),
                                                                                 publisher.identifier()));
    }

    public void traceReceive(Reactive.Publisher<INPUT> publisher, INPUT packet) {
        context.tracer().ifPresent(tracer -> tracer.receive(publisher.identifier(), subscriber.identifier(), packet));
    }

    public void rePullPublisher(Reactive.Publisher<INPUT> publisher) {
        subscriber.processor().schedulePullRetry(publisher, subscriber);
    }
}
