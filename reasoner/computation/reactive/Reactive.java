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
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface Reactive {

    String groupName();

    interface Provider<R> extends Reactive {

        void pull(Receiver<R> receiver);  // Should be idempotent if already pulling

        interface Publisher<T> extends Provider<T> {

            void publishTo(Receiver.Subscriber<T> subscriber);

            AbstractReactiveStream<T, T> findFirst();

            <R> AbstractReactiveStream<T, R> map(Function<T, R> function);

            <R> AbstractReactiveStream<T, R> flatMapOrRetry(Function<T, FunctionalIterator<R>> function);

            AbstractReactiveStream<T, T> buffer();

        }

        interface ReceiverRegistry<R> {

            void recordReceive();

            boolean isPulling();

            boolean addReceiver(Receiver<R> subscriber);

        }

        class SingleReceiverRegistry<R> implements ReceiverRegistry<R> {

            private Receiver<R> receiver;
            private boolean isPulling;

            public SingleReceiverRegistry(Receiver<R> receiver) {
                this.receiver = receiver;
                this.isPulling = false;
            }

            SingleReceiverRegistry() {
                this.receiver = null;
                this.isPulling = false;
            }

            @Override
            public void recordReceive() {
                isPulling = false;
            }

            public boolean recordPull() {
                boolean newPull = !isPulling;
                isPulling = true;
                return newPull;
            }

            @Override
            public boolean isPulling() {
                return isPulling;
            }

            @Override
            public boolean addReceiver(Receiver<R> receiver) {
                assert this.receiver == null;
                this.receiver = receiver;
                return true;
            }

            public Receiver<R> receiver() {
                assert this.receiver != null;
                return receiver;
            }
        }

        class MultiReceiverRegistry<R> implements ReceiverRegistry<R> {

            private final Set<Receiver<R>> receivers;
            private final Set<Receiver<R>> pullingReceivers;

            public MultiReceiverRegistry() {
                this.receivers = new HashSet<>();
                this.pullingReceivers = new HashSet<>();
            }

            @Override
            public void recordReceive() {
                pullingReceivers.clear();
            }

            void recordPull(Receiver<R> receiver) {
                pullingReceivers.add(receiver);
            }

            @Override
            public boolean isPulling() {
                return pullingReceivers.size() > 0;
            }

            @Override
            public boolean addReceiver(Receiver<R> receiver) {
                return receivers.add(receiver);
            }

            public int size() {
                return receivers.size();
            }

            public Set<Receiver<R>> pullingReceivers() {
                return new HashSet<>(pullingReceivers);
            }
        }

    }

    interface Receiver<R> extends Reactive {

        void receive(Provider<R> provider, R packet);  // TODO: The provider argument is only needed by compound - can we do without it?

        interface Subscriber<T> extends Receiver<T> {

            void subscribeTo(Provider<T> publisher);

        }

        interface ProviderRegistry<R> {
            // TODO: Consider managing whether to pull on upstreams by telling the manager whether we are pulling or not
            void add(Provider<R> provider);

            void pull(Provider<R> provider);

            void pullAll();

            void recordReceive(Provider<R> provider);  // TODO: This can likely be replaced/removed

        }

        class SingleProviderRegistry<R> implements ProviderRegistry<R> {

            private Provider<R> provider;
            private final Receiver<R> receiver;
            private boolean isPulling;

            public SingleProviderRegistry(Provider.Publisher<R> provider, Receiver<R> receiver) {
                this.provider = provider;
                this.receiver = receiver;
                this.isPulling = false;
            }

            public SingleProviderRegistry(Receiver<R> receiver) {
                this.provider = null;
                this.receiver = receiver;
                this.isPulling = false;
            }

            @Override
            public void add(Provider<R> provider) {
                assert this.provider == null || provider == this.provider;  // TODO: Add proper exception for trying to add more than one provider. Ideally only allow setting provider once
                this.provider = provider;
            }

            @Override
            public void pullAll() {
                if (provider != null) pull(provider);
            }

            @Override
            public void pull(Provider<R> provider) {
                assert this.provider != null;
                assert this.provider == provider;
                if (!isPulling()) {
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
                assert this.provider != null;
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
                provider.pull(receiver);
            }

            @Override
            public void recordReceive(Provider<R> provider) {
                assert this.provider == provider;
                setPulling(false);
            }
        }

        class MultiProviderRegistry<R> implements ProviderRegistry<R> {

            private final Map<Provider<R>, Boolean> providers;
            private final Receiver<R> receiver;

            public MultiProviderRegistry(Subscriber<R> subscriber) {
                this.providers = new HashMap<>();
                this.receiver = subscriber;
            }

            @Override
            public void add(Provider<R> provider) {
                providers.putIfAbsent(provider, false);
            }

            @Override
            public void recordReceive(Provider<R> provider) {
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
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
                provider.pull(receiver);
            }

            public int size() {
                return providers.size();
            }
        }
    }

}
