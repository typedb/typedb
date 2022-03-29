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

import java.util.HashSet;
import java.util.Set;

public abstract class ReceiverRegistry<RECEIVER> {

    abstract void setNotPulling();

    abstract boolean addReceiver(RECEIVER receiver);

    public static class Single<RECEIVER> extends ReceiverRegistry<RECEIVER> {

        private boolean isPulling;
        private RECEIVER receiver;

        public Single() {
            this.receiver = null;
            this.isPulling = false;
        }

        @Override
        public void setNotPulling() {
            isPulling = false;
        }

        public void recordPull(RECEIVER receiver) {
            assert this.receiver.equals(receiver);
            isPulling = true;
        }

        public boolean isPulling() {
            return isPulling;
        }

        @Override
        public boolean addReceiver(RECEIVER receiver) {
            assert this.receiver == null;
            this.receiver = receiver;
            return false;
        }

        public RECEIVER receiver() {
            assert this.receiver != null;
            return receiver;
        }
    }

    public static class Multi<RECEIVER> extends ReceiverRegistry<RECEIVER> {

        private final Set<RECEIVER> receivers;
        private final Set<RECEIVER> pullingReceivers;

        public Multi() {
            this.receivers = new HashSet<>();
            this.pullingReceivers = new HashSet<>();
        }

        @Override
        public void setNotPulling() {
            pullingReceivers.clear();
        }

        public void recordPull(RECEIVER receiver) {
            assert receivers.contains(receiver);
            pullingReceivers.add(receiver);
        }

        public boolean isPulling() {
            return pullingReceivers.size() > 0;
        }

        @Override
        public boolean addReceiver(RECEIVER receiver) {
            return receivers.add(receiver);
        }

        public Set<RECEIVER> pullingReceivers() {
            return new HashSet<>(pullingReceivers);
        }

        public int size() {
            return receivers.size();
        }
    }
}
