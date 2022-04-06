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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class ProviderRegistry<PROVIDER> {  // TODO: Rename Publisher Registry

    public abstract boolean add(PROVIDER provider);

    public abstract void recordReceive(PROVIDER provider);

    public abstract boolean setPulling(PROVIDER provider);

    public abstract Set<PROVIDER> nonPulling();

    public static class Single<PROVIDER> extends ProviderRegistry<PROVIDER> {

        private PROVIDER provider;
        private boolean isPulling;

        public Single() {
            this.provider = null;
            this.isPulling = false;
        }

        @Override
        public boolean add(PROVIDER provider) {
            assert provider != null;
            assert this.provider == null || provider == this.provider;  // TODO: Tighten this to allow adding only once
            boolean isNew = this.provider == null;
            this.provider = provider;
            return isNew;
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

        @Override
        public boolean setPulling(PROVIDER provider) {
            assert this.provider.equals(provider);
            return setPulling();
        }

        @Override
        public Set<PROVIDER> nonPulling() {
            assert provider != null;
            if (isPulling) return set();
            else return set(provider);
        }

        public PROVIDER provider() {
            return provider;
        }

    }

    public static class Multi<PROVIDER> extends ProviderRegistry<PROVIDER> {

        private final Map<PROVIDER, Boolean> providerPullState;

        public Multi() {
            this.providerPullState = new HashMap<>();
        }

        @Override
        public boolean add(PROVIDER provider) {
            assert provider != null;
            return providerPullState.putIfAbsent(provider, false) == null;
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

        @Override
        public boolean setPulling(PROVIDER provider) {
            assert providerPullState.containsKey(provider);
            Boolean wasPulling = providerPullState.put(provider, true);
            assert wasPulling != null;
            return !wasPulling;
        }

        public int size() {
            return providerPullState.size();
        }

        @Override
        public Set<PROVIDER> nonPulling() {
            Set<PROVIDER> nonPulling = new HashSet<>();
            providerPullState.keySet().forEach(p -> {
                if (setPulling(p)) nonPulling.add(p);
            });
            return nonPulling;
        }
    }

}
