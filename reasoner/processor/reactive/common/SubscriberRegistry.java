/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive.common;

import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Subscriber;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class SubscriberRegistry<PACKET> {

    public abstract void addSubscriber(Subscriber<PACKET> subscriber);

    public abstract Set<Subscriber<PACKET>> pulling();

    public abstract void setNotPulling(Subscriber<PACKET> subscriber);

    public abstract boolean anyPulling();

    public abstract Set<Subscriber<PACKET>> subscribers();

    public abstract void recordPull(Subscriber<PACKET> subscriber);

    public static class Single<PACKET> extends SubscriberRegistry<PACKET> {

        private boolean isPulling;
        private Subscriber<PACKET> subscriber;

        public Single() {
            this.subscriber = null;
            this.isPulling = false;
        }

        @Override
        public Set<Subscriber<PACKET>> subscribers() {
            return set(subscriber);
        }

        @Override
        public void recordPull(Subscriber<PACKET> subscriber) {
            assert this.subscriber.equals(subscriber);
            isPulling = true;
        }

        @Override
        public void addSubscriber(Subscriber<PACKET> subscriber) {
            assert this.subscriber == null;
            this.subscriber = subscriber;
        }

        @Override
        public Set<Subscriber<PACKET>> pulling() {
            if (isPulling) return set(subscriber);
            else return set();
        }

        @Override
        public void setNotPulling(Subscriber<PACKET> subscriber) {
            assert subscriber.equals(this.subscriber);
            isPulling = false;
        }

        @Override
        public boolean anyPulling() {
            return isPulling;
        }

    }

    public static class Multi<PACKET> extends SubscriberRegistry<PACKET> {

        private final Set<Subscriber<PACKET>> subscribers;
        private final Set<Subscriber<PACKET>> pullingSubscribers;

        public Multi() {
            this.subscribers = new HashSet<>();
            this.pullingSubscribers = new HashSet<>();
        }

        @Override
        public void setNotPulling(Subscriber<PACKET> subscriber) {
            pullingSubscribers.remove(subscriber);
        }

        @Override
        public void recordPull(Subscriber<PACKET> subscriber) {
            assert subscribers.contains(subscriber);
            pullingSubscribers.add(subscriber);
        }

        @Override
        public void addSubscriber(Subscriber<PACKET> subscriber) {
            subscribers.add(subscriber);
        }

        @Override
        public Set<Subscriber<PACKET>> pulling() {
            return new HashSet<>(pullingSubscribers);
        }

        @Override
        public boolean anyPulling() {
            return pullingSubscribers.size() > 0;
        }

        @Override
        public Set<Subscriber<PACKET>> subscribers() {
            return subscribers;
        }

    }
}
