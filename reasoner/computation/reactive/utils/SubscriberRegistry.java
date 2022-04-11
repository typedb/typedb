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

package com.vaticle.typedb.core.reasoner.computation.reactive.utils;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class SubscriberRegistry<SUBSCRIBER> {  // TODO: Rename SubscriberRegistry

    abstract void setNotPulling();

    public abstract boolean addSubscriber(SUBSCRIBER subscriber);

    public abstract Set<SUBSCRIBER> pulling();

    public abstract void setNotPulling(SUBSCRIBER subscriber);

    public abstract boolean anyPulling();

    public abstract Set<SUBSCRIBER> subscribers();

    public abstract void recordPull(SUBSCRIBER subscriber);

    public static class Single<SUBSCRIBER> extends SubscriberRegistry<SUBSCRIBER> {

        private boolean isPulling;
        private SUBSCRIBER subscriber;

        public Single() {
            this.subscriber = null;
            this.isPulling = false;
        }

        @Override
        public Set<SUBSCRIBER> subscribers() {
            return set(subscriber);
        }

        @Override
        public void setNotPulling() {
            isPulling = false;
        }

        @Override
        public void recordPull(SUBSCRIBER subscriber) {
            assert this.subscriber.equals(subscriber);
            isPulling = true;
        }

        public boolean isPulling() {
            return isPulling;
        }

        @Override
        public boolean addSubscriber(SUBSCRIBER subscriber) {
            assert this.subscriber == null;
            this.subscriber = subscriber;
            return false;
        }

        @Override
        public Set<SUBSCRIBER> pulling() {
            if (isPulling) return set(subscriber);
            else return set();
        }

        @Override
        public void setNotPulling(SUBSCRIBER subscriber) {
            assert subscriber.equals(this.subscriber);
            isPulling = false;
        }

        @Override
        public boolean anyPulling() {
            return isPulling;
        }

    }

    public static class Multi<RECEIVER> extends SubscriberRegistry<RECEIVER> {

        private final Set<RECEIVER> subscribers;
        private final Set<RECEIVER> pullingSubscribers;

        public Multi() {
            this.subscribers = new HashSet<>();
            this.pullingSubscribers = new HashSet<>();
        }

        @Override
        public void setNotPulling() {
            pullingSubscribers.clear();
        }

        @Override
        public void setNotPulling(RECEIVER subscriber) {
            pullingSubscribers.remove(subscriber);
        }

        @Override
        public void recordPull(RECEIVER subscriber) {
            assert subscribers.contains(subscriber);
            pullingSubscribers.add(subscriber);
        }

        @Override
        public boolean addSubscriber(RECEIVER subscriber) {
            return subscribers.add(subscriber);
        }

        @Override
        public Set<RECEIVER> pulling() {
            return new HashSet<>(pullingSubscribers);
        }

        @Override
        public boolean anyPulling() {
            return pullingSubscribers.size() > 0;
        }

        @Override
        public Set<RECEIVER> subscribers() {
            return subscribers;
        }

    }
}
