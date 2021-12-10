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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.reactive.Receiver.Subscriber;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class PublisherImpl<OUTPUT> implements Provider.Publisher<OUTPUT> {

    protected final Set<Receiver<OUTPUT>> subscribers;

    protected PublisherImpl() {
        this.subscribers = new HashSet<>();
    }

    @Override
    public void publishTo(Subscriber<OUTPUT> subscriber) {
        subscribers.add(subscriber);
        subscriber.subscribeTo(this);
        // TODO: To dynamically add subscribers we need to have buffered all prior packets and send them here
        //  we can adopt a policy that if you weren't a subscriber in time for the packet then you miss it, and
        //  break this only for outlets which will do the buffering and ensure all subscribers receive all answers.
    }

    protected Set<Receiver<OUTPUT>> subscribers() {
        return subscribers;
    }

    @Override
    public Reactive<OUTPUT, OUTPUT> findFirst() {
        return new FindFirstReactive<>(set(this));
    }

    @Override
    public <R> Reactive<OUTPUT, R> map(Function<OUTPUT, R> function) {
        return new MapReactive<>(set(this), function);
    }

    @Override
    public <R> Reactive<OUTPUT, R> flatMapOrRetry(Function<OUTPUT, FunctionalIterator<R>> function) {
        return new FlatMapOrRetryReactive<>(set(this), function);
    }
}
