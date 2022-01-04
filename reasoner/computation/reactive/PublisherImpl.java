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
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver.Subscriber;

import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class PublisherImpl<OUTPUT> implements Provider.Publisher<OUTPUT> {

    protected Receiver<OUTPUT> subscriber;

    @Override
    public void publishTo(Subscriber<OUTPUT> subscriber) {
        setSubscriber(subscriber);
        subscriber.subscribeTo(this);
    }

    protected void setSubscriber(Receiver<OUTPUT> subscriber) {
        // TODO: This is duplicated in the Reactive class hierarchy
        assert this.subscriber == null;
        this.subscriber = subscriber;
    }

    protected Receiver<OUTPUT> subscriber() {
        return subscriber;
    }

    @Override
    public ReactiveBase<OUTPUT, OUTPUT> findFirst() {
        return new FindFirstReactive<>(set(this));
    }

    @Override
    public <R> ReactiveBase<OUTPUT, R> map(Function<OUTPUT, R> function) {
        return new MapReactive<>(set(this), function);
    }

    @Override
    public <R> ReactiveBase<OUTPUT, R> flatMapOrRetry(Function<OUTPUT, FunctionalIterator<R>> function) {
        return new FlatMapOrRetryReactive<>(set(this), function);
    }
}
