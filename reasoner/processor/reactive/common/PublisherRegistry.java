/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;

import java.util.HashMap;
import java.util.Map;

import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class PublisherRegistry<PACKET> {

    public abstract void add(Publisher<PACKET> publisher);

    public abstract void recordReceive(Publisher<PACKET> publisher);

    public abstract boolean setPulling(Publisher<PACKET> publisher);

    public abstract FunctionalIterator<Publisher<PACKET>> nonPulling();

    public abstract int size();

    public abstract boolean contains(Publisher<PACKET> publisher);

    public static class Single<PACKET> extends PublisherRegistry<PACKET> {

        private Publisher<PACKET> publisher;
        private boolean isPulling;

        public Single() {
            this.publisher = null;
            this.isPulling = false;
        }

        @Override
        public void add(Publisher<PACKET> publisher) {
            assert publisher != null;
            assert this.publisher == null;
            this.publisher = publisher;
        }

        public boolean setPulling() {
            boolean wasPulling = isPulling;
            isPulling = true;
            return !wasPulling;
        }

        @Override
        public void recordReceive(Publisher<PACKET> publisher) {
            assert this.publisher == publisher;
            isPulling = false;
        }

        @Override
        public boolean setPulling(Publisher<PACKET> publisher) {
            assert this.publisher.equals(publisher);
            return setPulling();
        }

        @Override
        public FunctionalIterator<Publisher<PACKET>> nonPulling() {
            assert publisher != null;
            if (isPulling) return empty();
            else return iterate(publisher);
        }

        @Override
        public int size() {
            if (publisher == null) return 0;
            else return 1;
        }

        @Override
        public boolean contains(Publisher<PACKET> publisher) {
            return this.publisher == publisher;
        }

        public Publisher<PACKET> publisher() {
            return publisher;
        }

    }

    public static class Multi<PACKET> extends PublisherRegistry<PACKET> {

        private final Map<Publisher<PACKET>, Boolean> publisherPullState;

        public Multi() {
            this.publisherPullState = new HashMap<>();
        }

        @Override
        public void add(Publisher<PACKET> publisher) {
            assert publisher != null;
            assert publisherPullState.get(publisher) == null;
            publisherPullState.put(publisher, false);
        }

        @Override
        public void recordReceive(Publisher<PACKET> publisher) {
            assert publisherPullState.containsKey(publisher);
            publisherPullState.put(publisher, false);
        }

        @Override
        public boolean setPulling(Publisher<PACKET> publisher) {
            assert publisherPullState.containsKey(publisher);
            Boolean wasPulling = publisherPullState.put(publisher, true);
            assert wasPulling != null;
            return !wasPulling;
        }

        @Override
        public int size() {
            return publisherPullState.size();
        }

        @Override
        public boolean contains(Publisher<PACKET> publisher) {
            return publisherPullState.containsKey(publisher);
        }

        @Override
        public FunctionalIterator<Publisher<PACKET>> nonPulling() {
            return iterate(publisherPullState.entrySet()).filter(e -> !e.getValue()).map(Map.Entry::getKey);
        }
    }
}
