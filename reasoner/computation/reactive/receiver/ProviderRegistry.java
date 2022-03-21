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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.HashMap;
import java.util.Map;

public abstract class ProviderRegistry<PROVIDER extends Reactive> {

    protected final Processor<?, ?, ?, ?> processor;

    protected ProviderRegistry(Processor<?, ?, ?, ?> processor) {
        this.processor = processor;
    }

    protected abstract Reactive.Receiver receiver();

    public abstract void add(PROVIDER provider);

    public abstract void pullAll();

    public abstract void recordReceive(PROVIDER provider);

    public void pull(PROVIDER provider) {
        pull(provider, false);
    }

    protected abstract void pull(PROVIDER provider, boolean async);

    abstract void pullProvider(PROVIDER provider, boolean async);

    public abstract <PROVIDER extends Reactive> void retry(PROVIDER provider);  // TODO: New mechanism required

    public static class SingleProviderRegistry<PROVIDER extends Reactive> extends ProviderRegistry<PROVIDER> {

        private PROVIDER provider;
        private boolean isPulling;

        public SingleProviderRegistry(PROVIDER provider, Reactive.Receiver receiver, Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.isPulling = false;
            add(provider);
        }

        public SingleProviderRegistry(Reactive.Receiver receiver, Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.provider = null;
            this.isPulling = false;
        }

        @Override
        public void add(PROVIDER provider) {
            assert provider != null;
            assert this.provider == null || provider == this.provider;  // TODO: Tighten this to allow adding only once
            if (this.provider == null) {
                processor.monitor().execute(actor -> actor.registerPath(receiver.identifier(), provider.identifier()));
            }
            this.provider = provider;
        }

        @Override
        public void pullAll() {
            if (provider != null) pull(provider);
        }

        @Override
        public void pull(PROVIDER provider, boolean async) {
            assert this.provider != null;
            assert this.provider == provider;
            if (!isPulling()) {
                setPulling(true);
                pullProvider(provider, async);
            }
        }

        public boolean isPulling() {
            return isPulling;
        }

        private void setPulling(boolean pulling) {
            isPulling = pulling;
        }

        @Override
        public void recordReceive(PROVIDER provider) {
            assert this.provider == provider;
            setPulling(false);
        }

        public static class Sync<PACKET> extends SingleProviderRegistry<Provider.Sync<PACKET>> {

            private final Reactive.Receiver.Sync<PACKET> receiver;

            public Sync(Provider.Sync<PACKET> packetSync, Reactive.Receiver.Sync<PACKET> receiver,
                        Processor<?, ?, ?, ?> processor) {
                super(packetSync, processor);
                this.receiver = receiver;
            }

            @Override
            void pullProvider(Provider.Sync<PACKET> provider, boolean async) {
                // TODO: if PROVIDER is a sync provider, then either pull async or sync depending on whether this is a retry. If it's an identifier for a provider then pull across the actor boundary. In that case we need the provider's processor
                Tracer.getIfEnabled().ifPresent(tracer -> tracer.pull(receiver, provider));
                if (async) processor.driver().execute(actor -> actor.retryPull(provider.identifier(), receiver().identifier()));
                else provider.pull(receiver());
            }

            @Override
            protected Reactive.Receiver.Sync<PACKET> receiver() {
                return receiver;
            }

            public void retry(Provider.Sync<PACKET> provider) {
                pull(provider, true);
            }
        }

    }

    public static class MultiProviderRegistry<PROVIDER extends Reactive> extends ProviderRegistry<PROVIDER> {

        private final Map<PROVIDER, Boolean> providerPullState;

        public MultiProviderRegistry(Reactive.Receiver.Sync<?> receiver, Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.providerPullState = new HashMap<>();
        }

        @Override
        public void add(PROVIDER provider) {
            assert provider != null;
            if (providerPullState.putIfAbsent(provider, false) == null) {
                processor.monitor().execute(actor -> actor.registerPath(receiver().identifier(), provider.identifier()));
            }
        }

        @Override
        public void recordReceive(PROVIDER provider) {
            setPulling(provider, false);
        }

        @Override
        public void pullAll() {
            providerPullState.keySet().forEach(this::pull);
        }

        @Override
        public void pull(PROVIDER provider, boolean async) {
            assert providerPullState.containsKey(provider);
            if (!isPulling(provider)) {
                setPulling(provider, true);
                pullProvider(provider, async);
            }
        }

        public boolean isPulling(PROVIDER provider) {
            assert providerPullState.containsKey(provider);
            return providerPullState.get(provider);
        }

        private void setPulling(PROVIDER provider, boolean isPulling) {
            assert providerPullState.containsKey(provider);
            providerPullState.put(provider, isPulling);
        }

        public int size() {
            return providerPullState.size();
        }
    }

}
