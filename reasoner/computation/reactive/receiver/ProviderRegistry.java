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

package com.vaticle.typedb.core.reasoner.computation.reactive.receiver;

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class ProviderRegistry<R> {
    // TODO: Consider managing whether to pull on upstreams by telling the manager whether we are pulling or not
    public abstract void add(Reactive.Provider<R> provider);

    public abstract void pull(Reactive.Provider<R> provider);

    public abstract void pullAll();

    public abstract void propagateMonitors(Set<Processor.Monitor.Reference> monitors);

    public abstract void recordReceive(Reactive.Provider<R> provider);

    public static class SingleProviderRegistry<R> extends ProviderRegistry<R> {

        private Reactive.Provider<R> provider;
        private final Reactive.Receiver<R> receiver;
        private boolean isPulling;

        public SingleProviderRegistry(Reactive.Provider.Publisher<R> provider, Reactive.Receiver<R> receiver) {
            this.provider = provider;
            this.receiver = receiver;
            this.isPulling = false;
        }

        public SingleProviderRegistry(Reactive.Receiver<R> receiver) {
            this.provider = null;
            this.receiver = receiver;
            this.isPulling = false;
        }

        @Override
        public void add(Reactive.Provider<R> provider) {
            assert this.provider == null || provider == this.provider;
            this.provider = provider;
        }

        @Override
        public void pullAll() {
            if (provider != null) pull(provider);
        }

        @Override
        public void propagateMonitors(Set<Processor.Monitor.Reference> monitors) {
            provider.propagateMonitors(receiver, monitors);
        }

        @Override
        public void pull(Reactive.Provider<R> provider) {
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
        public void recordReceive(Reactive.Provider<R> provider) {
            assert this.provider == provider;
            setPulling(false);
        }
    }

    public static class MultiProviderRegistry<R> extends ProviderRegistry<R> {

        private final Map<Reactive.Provider<R>, Boolean> providerPullState;
        private final Set<Processor.Monitor.Reference> monitors;
        private final Reactive.Receiver<R> receiver;

        public MultiProviderRegistry(Reactive.Receiver.Subscriber<R> subscriber) {
            this.providerPullState = new HashMap<>();
            this.receiver = subscriber;
            this.monitors = new HashSet<>();
        }

        @Override
        public void add(Reactive.Provider<R> provider) {
            providerPullState.putIfAbsent(provider, false);
            // propagateMonitors(monitors);  // TODO: This can propagate monitors ahead of pulls
        }

        @Override
        public void recordReceive(Reactive.Provider<R> provider) {
            setPulling(provider, false);
        }

        @Override
        public void pullAll() {
            providerPullState.keySet().forEach(this::pull);
        }

        @Override
        public void pull(Reactive.Provider<R> provider) {
            assert providerPullState.containsKey(provider);
            if (!isPulling(provider)) {
                setPulling(provider, true);
                pullProvider(provider);
            }
        }

        @Override
        public void propagateMonitors(Set<Processor.Monitor.Reference> monitors) {
            Set<Processor.Monitor.Reference> toPropagate = new HashSet<>(monitors);
            toPropagate.removeAll(this.monitors);
            providerPullState.keySet().forEach(provider -> provider.propagateMonitors(receiver, toPropagate));
        }

        private boolean isPulling(Reactive.Provider<R> provider) {
            assert providerPullState.containsKey(provider);
            return providerPullState.get(provider);
        }

        private void setPulling(Reactive.Provider<R> provider, boolean isPulling) {
            assert providerPullState.containsKey(provider);
            providerPullState.put(provider, isPulling);
        }

        private void pullProvider(Reactive.Provider<R> provider) {
            assert providerPullState.containsKey(provider);
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
            provider.pull(receiver);
        }

        public int size() {
            return providerPullState.size();
        }
    }
}
