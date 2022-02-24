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

package com.vaticle.typedb.core.reasoner.computation.reactive.provider;

import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;

import java.util.HashSet;
import java.util.Set;

public abstract class ReceiverRegistry<R> {

    abstract void recordReceive();

    abstract boolean isPulling();

    abstract boolean addReceiver(Reactive.Receiver<R> subscriber);

    public static class SingleReceiverRegistry<R> extends ReceiverRegistry<R> {

        private Reactive.Receiver<R> receiver;
        private boolean isPulling;

        public SingleReceiverRegistry(Reactive.Receiver<R> receiver) {
            this.receiver = receiver;
            this.isPulling = false;
        }

        public SingleReceiverRegistry() {
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
        public boolean addReceiver(Reactive.Receiver<R> receiver) {
            assert this.receiver == null;
            this.receiver = receiver;
            return true;
        }

        public Reactive.Receiver<R> receiver() {
            assert this.receiver != null;
            return receiver;
        }
    }

    public static class MultiReceiverRegistry<R> extends ReceiverRegistry<R> {

        private final Set<Reactive.Receiver<R>> receivers;
        private final Set<Reactive.Receiver<R>> pullingReceivers;

        public MultiReceiverRegistry() {
            this.receivers = new HashSet<>();
            this.pullingReceivers = new HashSet<>();
        }

        @Override
        public void recordReceive() {
            pullingReceivers.clear();
        }

        public void recordPull(Reactive.Receiver<R> receiver) {
            pullingReceivers.add(receiver);
        }

        @Override
        public boolean isPulling() {
            return pullingReceivers.size() > 0;
        }

        @Override
        public boolean addReceiver(Reactive.Receiver<R> receiver) {
            return receivers.add(receiver);
        }

        public int size() {
            return receivers.size();
        }

        public Set<Reactive.Receiver<R>> pullingReceivers() {
            return new HashSet<>(pullingReceivers);
        }
    }
}
