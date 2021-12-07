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

import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class ReactiveImpl<INPUT, OUTPUT> implements Reactive<INPUT, OUTPUT> {  // TODO: Pull out an interface for Reactive

    protected final Set<Subscriber<OUTPUT>> subscribers;
    private final Set<Publisher<INPUT>> publishers;
    protected boolean isPulling;

    ReactiveImpl(Set<Subscriber<OUTPUT>> subscribers, Set<Publisher<INPUT>> publishers) {
        this.subscribers = subscribers;
        this.publishers = publishers;
        this.isPulling = false;
    }

    @Override
    public void pull(Subscriber<OUTPUT> subscriber) {
        publish(subscriber);
        if (!isPulling) {
            publishers.forEach(this::publisherPull);
            isPulling = true;
        }
    }

    protected Set<Subscriber<OUTPUT>> subscribers() {
        return subscribers;
    }

    protected Set<Publisher<INPUT>> publishers() {
        return publishers;
    }

    @Override
    public void publish(Subscriber<OUTPUT> subscriber) {
        subscribers.add(subscriber);
        // TODO: To dynamically add subscribers we need to have buffered all prior packets and send them here
        //  we can adopt a policy that if you weren't a subscriber in time for the packet then you miss it, and
        //  break this only for outlets which will do the buffering and ensure all subscribers receive all answers.
    }

    @Override
    public Publisher<INPUT> subscribe(Publisher<INPUT> publisher) {
        publishers.add(publisher);
        if (isPulling) publisher.pull(this);
        return publisher;
    }

    protected void subscriberReceive(Subscriber<OUTPUT> subscriber, OUTPUT p) {
        subscriber.receive(this, p);
    }

    protected void publisherPull(Publisher<INPUT> publisher) {
        publisher.pull(this);
    }

    @Override
    public FindFirstReactive<INPUT> findFirstSubscribe() {
        FindFirstReactive<INPUT> newReactive = new FindFirstReactive<>(set(this), set());
        subscribe(newReactive);
        return newReactive;
    }

    @Override
    public <PUB_INPUT> MapReactive<PUB_INPUT, INPUT> mapSubscribe(Function<PUB_INPUT, INPUT> function) {
        MapReactive<PUB_INPUT, INPUT> newReactive = new MapReactive<>(set(this), set(), function);
        subscribe(newReactive);
        return newReactive;
    }

    @Override
    public <PUB_INPUT> FlatMapOrRetryReactive<PUB_INPUT, INPUT> flatMapOrRetrySubscribe(Function<PUB_INPUT, FunctionalIterator<INPUT>> function) {
        FlatMapOrRetryReactive<PUB_INPUT, INPUT> newReactive = new FlatMapOrRetryReactive<>(set(this), set(), function);
        subscribe(newReactive);
        return newReactive;
    }

}
