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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;

import java.util.function.Supplier;

public class Source<PACKET> extends SingleReceiverPublisher<PACKET> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private boolean exhausted;
    private FunctionalIterator<PACKET> iterator;

    public Source(Supplier<FunctionalIterator<PACKET>> iteratorSupplier, Monitor.MonitorRef monitor, String groupName) {
        super(monitor, groupName);
        this.iteratorSupplier = iteratorSupplier;
        this.exhausted = false;
        monitor().registerSource(this);
    }

    public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier,
                                                             Monitor.MonitorRef monitor, String groupName) {
        return new Source<>(iteratorSupplier, monitor, groupName);
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        assert receiver.equals(receiverRegistry().receiver());
        receiverRegistry().recordPull(receiver);
        if (!exhausted) {
            if (iterator == null) iterator = iteratorSupplier.get();
            if (iterator.hasNext()) {
                monitor().createAnswer(this);
                receiver.receive(this, iterator.next());
            } else {
                exhausted = true;
                monitor().sourceFinished(this);
            }
        }
    }

}
