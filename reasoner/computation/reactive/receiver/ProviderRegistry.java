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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class ProviderRegistry<PROVIDER extends Reactive> {

    protected final Processor<?, ?, ?, ?> processor;

    protected ProviderRegistry(Processor<?, ?, ?, ?> processor) {
        this.processor = processor;
    }

    public abstract void add(PROVIDER provider);

    public abstract void recordReceive(PROVIDER provider);

    public static class Single<PROVIDER extends Reactive> extends ProviderRegistry<PROVIDER> {

        private final Reactive owner;
        private PROVIDER provider;
        private boolean isPulling;

        public Single(PROVIDER provider, Reactive owner, Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.owner = owner;
            this.isPulling = false;
            add(provider);
        }

        public Single(Reactive owner, Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.owner = owner;
            this.provider = null;
            this.isPulling = false;
        }

        @Override
        public void add(PROVIDER provider) {
            assert provider != null;
            assert this.provider == null || provider == this.provider;  // TODO: Tighten this to allow adding only once
            if (this.provider == null) {
                processor.monitor().execute(actor -> actor.registerPath(owner.identifier(), provider.identifier()));
            }
            this.provider = provider;
        }

        public boolean setPulling() {
            boolean wasPulling = isPulling;
            isPulling = true;
            return !wasPulling;
        }

        @Override
        public void recordReceive(PROVIDER provider) {
            assert this.provider == provider;
            isPulling = false;
        }

        public PROVIDER provider() {
            return provider;
        }

    }

    public static class Multi<PROVIDER extends Reactive> extends ProviderRegistry<PROVIDER> {

        private final Map<PROVIDER, Boolean> providerPullState;
        private final Reactive owner;

        public Multi(Reactive owner, Processor<?, ?, ?, ?> processor) {
            super(processor);
            this.owner = owner;
            this.providerPullState = new HashMap<>();
        }

        @Override
        public void add(PROVIDER provider) {
            assert provider != null;
            if (providerPullState.putIfAbsent(provider, false) == null) {
                processor.monitor().execute(actor -> actor.registerPath(owner.identifier(), provider.identifier()));
            }
        }

        @Override
        public void recordReceive(PROVIDER provider) {
            assert providerPullState.containsKey(provider);
            providerPullState.put(provider, false);
        }

        public boolean isPulling(PROVIDER provider) {
            assert providerPullState.containsKey(provider);
            return providerPullState.get(provider);
        }

        public boolean setPulling(PROVIDER provider) {
            assert providerPullState.containsKey(provider);
            Boolean wasPulling = providerPullState.put(provider, true);
            assert wasPulling != null;
            return !wasPulling;
        }

        public int size() {
            return providerPullState.size();
        }

        public Set<PROVIDER> nonPulling() {
            Set<PROVIDER> nonPulling = new HashSet<>();
            providerPullState.keySet().forEach(p -> {
                if (setPulling(p)) nonPulling.add(p);
            });
            return nonPulling;
        }
    }

}
