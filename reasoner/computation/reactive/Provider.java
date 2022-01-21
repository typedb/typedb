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

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public interface Provider<R> {

    void pull(Receiver<R> receiver);

    String groupName();

    interface Publisher<T> extends Provider<T> {

        void publishTo(Subscriber<T> subscriber);

        ReactiveBase<T, T> findFirst();

        <R> ReactiveBase<T, R> map(Function<T, R> function);

        <R> ReactiveBase<T, R> flatMapOrRetry(Function<T, FunctionalIterator<R>> function);

    }

    interface Manager<R> {
        // TODO: Consider managing whether to pull on upstreams by telling the manager whether we are pulling or not
        void add(Provider<R> provider);
        void pull(Provider<R> provider);
        void pullAll();
        int size();
    }

    class SingleManager<R> implements Manager<R> {

        private @Nullable Provider<R> provider;
        private final Receiver<R> receiver;

        public SingleManager(@Nullable Publisher<R> provider, Subscriber<R> subscriber) {
            this.provider = provider;
            this.receiver = subscriber;
        }

        public SingleManager(Subscriber<R> subscriber) {
            this.provider = null;
            this.receiver = subscriber;
        }

        @Override
        public void add(Provider<R> provider) {
            assert this.provider == null || provider == this.provider;  // TODO: Add proper exception for trying to add more than one provider. Ideally only allow setting provider once
            this.provider = provider;
        }

        @Override
        public void pull(Provider<R> provider) {
            assert this.provider != null;
            assert this.provider == provider;
            provider.pull(receiver);
        }

        @Override
        public void pullAll() {
            if (provider != null) {
                provider.pull(receiver);
            }
        }

        @Override
        public int size() {
            if (provider == null) return 0;
            else return 1;
        }
    }

    class MultiManager<R> implements Manager<R> {

        private final Set<Provider<R>> providers;
        private final Receiver<R> receiver;
        private final PacketMonitor monitor;
        private boolean hasForked;

        public MultiManager(Subscriber<R> subscriber, PacketMonitor monitor) {
            this.providers = new HashSet<>();
            this.receiver = subscriber;
            this.monitor = monitor;
            this.hasForked = false;
        }

        public static <R> MultiManager<R> create(Subscriber<R> subscriber, PacketMonitor monitor) {
            return new MultiManager<>(subscriber, monitor);
        }

        @Override
        public void add(Provider<R> provider) {
            providers.add(provider);
        }

        // TODO: Duplicated from ReactiveImpl
        @Override
        public void pullAll() {
            providers.forEach(this::pull);
        }

        @Override
        public int size() {
            return providers.size();
        }

        // TODO: Duplicated from ReactiveImpl
        @Override
        public void pull(Provider<R> provider) {
            if (hasForked) monitor.onPathFork(1);
            provider.pull(receiver);
            hasForked = true;
        }
    }
}
