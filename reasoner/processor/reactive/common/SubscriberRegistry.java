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
