/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive.common;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
            List<Publisher<PACKET>> nonPulling = new ArrayList<>();
            publisherPullState.entrySet().forEach(e -> {
                if (!e.getValue()) nonPulling.add(e.getKey());
            });
            return iterate(nonPulling);
        }
    }
}
