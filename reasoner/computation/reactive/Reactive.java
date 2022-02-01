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
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface Reactive {

    String groupName();

    interface Receiver<R> extends Reactive {

        void receive(Provider<R> provider, R packet);  // TODO: The provider argument is only needed by compound - can we do without it?

        interface Subscriber<T> extends Receiver<T> {

            void subscribeTo(Provider<T> publisher);

        }
    }

    interface Provider<R> extends Reactive {

        void pull(Receiver<R> receiver);  // Should be idempotent if already pulling

        interface Publisher<T> extends Provider<T> {

            void publishTo(Receiver.Subscriber<T> subscriber);

            ReactiveStream<T, T> findFirst();

            <R> ReactiveStream<T, R> map(Function<T, R> function);

            <R> ReactiveStream<T, R> flatMapOrRetry(Function<T, FunctionalIterator<R>> function);

            ReactiveStream<T, T> buffer();

        }

        interface Manager<R> {
            // TODO: Consider managing whether to pull on upstreams by telling the manager whether we are pulling or not
            void add(Provider<R> provider);

            void pull(Provider<R> provider);

            void pullAll();

            int size();

            void receivedFrom(Provider<R> provider);

        }

        class SingleManager<R> implements Manager<R> {
            // TODO: isPulling and setPulling methods should be managed here

            private @Nullable
            Provider<R> provider;
            private final Receiver<R> receiver;
            private final Monitoring monitor;
            private boolean isPulling;

            public SingleManager(@Nullable Publisher<R> provider, Receiver.Subscriber<R> subscriber, Monitoring monitor) {
                this.provider = provider;
                this.receiver = subscriber;
                this.monitor = monitor;
                this.isPulling = false;
            }

            public SingleManager(Receiver.Subscriber<R> subscriber, Monitoring monitor) {
                this.provider = null;
                this.receiver = subscriber;
                this.monitor = monitor;
                this.isPulling = false;
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
                if (!isPulling()) {  // TODO: In most reactives we already do this defence, now we can do it once here abstractly.
                    setPulling(true);
                    pullProvider();
                }
            }

            private boolean isPulling() {
                return isPulling;
            }

            private void setPulling(boolean pulling) {
                isPulling = pulling;
            }

            private void pullProvider() {
                if (monitor == null) Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
                else Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider, monitor.count()));
                assert this.provider != null;
                provider.pull(receiver);
            }

            @Override
            public void pullAll() {
                if (provider != null) pull(provider);
            }

            @Override
            public int size() {
                if (provider == null) return 0;
                else return 1;
            }

            @Override
            public void receivedFrom(Provider<R> provider) {
                assert this.provider == provider;
                setPulling(false);
            }
        }

        class MultiManager<R> implements Manager<R> {

            private final Map<Provider<R>, Boolean> providers;
            private final Receiver<R> receiver;
            private final Monitoring monitor;

            public MultiManager(Receiver.Subscriber<R> subscriber, @Nullable Monitoring monitor) {
                this.providers = new HashMap<>();
                this.receiver = subscriber;
                this.monitor = monitor;
            }

            @Override
            public void add(Provider<R> provider) {
                providers.putIfAbsent(provider, false);
            }

            public void finaliseProviders() {
                assert monitor != null;
                final int numForks = providers.size() - 1;
                if (numForks > 0) monitor.onPathFork(numForks, receiver);
            }

            @Override
            public int size() {
                return providers.size();
            }

            @Override
            public void receivedFrom(Provider<R> provider) {
                setPulling(provider, false);
            }

            @Override
            public void pullAll() {
                providers.keySet().forEach(this::pull);
            }

            @Override
            public void pull(Provider<R> provider) {
                assert providers.containsKey(provider);
                if (!isPulling(provider)) {
                    setPulling(provider, true);
                    pullProvider(provider);
                }
            }

            private boolean isPulling(Provider<R> provider) {
                assert providers.containsKey(provider);
                return providers.get(provider);
            }

            private void setPulling(Provider<R> provider, boolean isPulling) {
                assert providers.containsKey(provider);
                providers.put(provider, isPulling);
            }

            private void pullProvider(Provider<R> provider) {
                assert providers.containsKey(provider);
                if (monitor == null) Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
                else Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider, monitor.count()));
                provider.pull(receiver);
            }
        }
    }
}
