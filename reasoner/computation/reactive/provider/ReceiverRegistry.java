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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public abstract class ReceiverRegistry<R> {

    abstract void recordReceive();

    abstract boolean addReceiver(Reactive.Receiver<R> receiver);

    public abstract Set<Processor.Monitor.Reference> monitors();

    public static class SingleReceiverRegistry<R> extends ReceiverRegistry<R> {

        private boolean isPulling;
        private final Set<Processor.Monitor.Reference> monitors;
        private Reactive.Receiver<R> receiver;

        public SingleReceiverRegistry(Reactive.Receiver<R> receiver) {
            this.receiver = receiver;
            this.isPulling = false;
            this.monitors = new HashSet<>();
        }

        public SingleReceiverRegistry() {
            this.receiver = null;
            this.isPulling = false;
            this.monitors = new HashSet<>();
        }

        @Override
        public void recordReceive() {
            isPulling = false;
        }

        public boolean recordPull(Set<Processor.Monitor.Reference> monitors) {
            boolean newRecord = this.monitors.addAll(monitors) || !isPulling; // TODO: If this pull includes new monitors (can it?) then should this method return true? Also if the pull isn't transmitted upstream then those reactives won't be aware of the new monitor(s)
            isPulling = true;
            return newRecord;
        }

        public boolean isPulling() {
            return isPulling;
        }

        @Override
        public boolean addReceiver(Reactive.Receiver<R> receiver) {
            assert this.receiver == null;
            this.receiver = receiver;
            return true;
        }

        @Override
        public Set<Processor.Monitor.Reference> monitors() {
            return monitors;
        }

        public Reactive.Receiver<R> receiver() {
            assert this.receiver != null;
            return receiver;
        }
    }

    public static class MultiReceiverRegistry<R> extends ReceiverRegistry<R> {

        private final Set<Reactive.Receiver<R>> receivers;
        private final Map<Processor.Monitor.Reference, Set<Reactive.Receiver<R>>> monitorReceivers;
        private final Set<Reactive.Receiver<R>> pullingReceivers;

        public MultiReceiverRegistry() {
            this.receivers = new HashSet<>();
            this.pullingReceivers = new HashSet<>();
            this.monitorReceivers = new HashMap<>();
        }

        @Override
        public void recordReceive() {
            pullingReceivers.clear();
        }

        public Set<Processor.Monitor.Reference> recordPull(Reactive.Receiver<R> receiver, Set<Processor.Monitor.Reference> monitors) {
            pullingReceivers.add(receiver);
            Set<Processor.Monitor.Reference> newMonitors = new HashSet<>();
            monitors.forEach(monitor -> {
                Set<Reactive.Receiver<R>> recs = monitorReceivers.computeIfAbsent(monitor, mon -> {
                    newMonitors.add(mon);
                    return new HashSet<>(set(receiver));
                });
                if (recs.add(receiver)) newMonitors.add(monitor);
            });
            return newMonitors;
        }

        public boolean isPulling() {
            return pullingReceivers.size() > 0;
        }

        @Override
        public boolean addReceiver(Reactive.Receiver<R> receiver) {
            return receivers.add(receiver);
        }

        @Override
        public Set<Processor.Monitor.Reference> monitors() {
            return monitorReceivers.keySet();
        }

        public int size(Processor.Monitor.Reference monitor) {
            return monitorReceivers.get(monitor).size();
        }

        public Set<Reactive.Receiver<R>> pullingReceivers() {
            return new HashSet<>(pullingReceivers);
        }
    }
}
