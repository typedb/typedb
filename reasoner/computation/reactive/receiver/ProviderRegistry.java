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
import java.util.Map;

public abstract class ProviderRegistry<R> {

    protected final Processor<?, ?, ?, ?> processor;
    protected final Reactive.Receiver<R> receiver;

    protected ProviderRegistry(Reactive.Receiver<R> receiver, Processor<?, ?, ?, ?> processor) {
        this.processor = processor;
        this.receiver = receiver;
    }

    // TODO: Consider managing whether to pull on upstreams by telling the manager whether we are pulling or not
    public abstract void add(Reactive.Provider<R> provider);

    public abstract void pullAll();

    public abstract void recordReceive(Reactive.Provider<R> provider);

    public void pull(Reactive.Provider<R> provider) {
        pull(provider, false);
    }

    public void retry(Reactive.Provider<R> provider) {
        pull(provider, true);
    }

    protected abstract void pull(Reactive.Provider<R> provider, boolean async);

    protected void pullProvider(Reactive.Provider<R> provider, boolean async) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
        if (async) processor.driver().execute(actor -> actor.pull(provider, receiver));
        else provider.pull(receiver);
    }

    public static class SingleProviderRegistry<R> extends ProviderRegistry<R> {

        private Reactive.Provider<R> provider;
        private boolean isPulling;

        public SingleProviderRegistry(Reactive.Provider.Publisher<R> provider, Reactive.Receiver<R> receiver,
                                      Processor<?, ?, ?, ?> processor) {
            super(receiver, processor);
            this.isPulling = false;
            add(provider);
        }

        public SingleProviderRegistry(Reactive.Receiver<R> receiver, Processor<?, ?, ?, ?> processor) {
            super(receiver, processor);
            this.provider = null;
            this.isPulling = false;
        }

        @Override
        public void add(Reactive.Provider<R> provider) {
            assert provider != null;
            assert this.provider == null || provider == this.provider;  // TODO: Tighten this to allow adding only once
            if (this.provider == null) processor.monitor().execute(actor -> actor.registerPath(receiver, provider));
            this.provider = provider;
        }

        @Override
        public void pullAll() {
            if (provider != null) pull(provider);
        }

        @Override
        public void pull(Reactive.Provider<R> provider, boolean async) {
            assert this.provider != null;
            assert this.provider == provider;
            if (!isPulling()) {
                setPulling(true);
                pullProvider(provider, async);
            }
        }

        private boolean isPulling() {
            return isPulling;
        }

        private void setPulling(boolean pulling) {
            isPulling = pulling;
        }

        @Override
        public void recordReceive(Reactive.Provider<R> provider) {
            assert this.provider == provider;
            setPulling(false);
        }
    }

    public static class MultiProviderRegistry<R> extends ProviderRegistry<R> {

        private final Map<Reactive.Provider<R>, Boolean> providerPullState;

        public MultiProviderRegistry(Reactive.Receiver<R> receiver, Processor<?, ?, ?, ?> processor) {
            super(receiver, processor);
            this.providerPullState = new HashMap<>();
        }

        @Override
        public void add(Reactive.Provider<R> provider) {
            assert provider != null;
            if (providerPullState.putIfAbsent(provider, false) == null) {
                processor.monitor().execute(actor -> actor.registerPath(receiver, provider));
            }
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
        public void pull(Reactive.Provider<R> provider, boolean async) {
            assert providerPullState.containsKey(provider);
            if (!isPulling(provider)) {
                setPulling(provider, true);
                pullProvider(provider, async);
            }
        }

        private boolean isPulling(Reactive.Provider<R> provider) {
            assert providerPullState.containsKey(provider);
            return providerPullState.get(provider);
        }

        private void setPulling(Reactive.Provider<R> provider, boolean isPulling) {
            assert providerPullState.containsKey(provider);
            providerPullState.put(provider, isPulling);
        }

        public int size() {
            return providerPullState.size();
        }
    }

}
