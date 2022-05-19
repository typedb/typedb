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
