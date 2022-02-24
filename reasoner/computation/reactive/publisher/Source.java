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

package com.vaticle.typedb.core.reasoner.computation.reactive.publisher;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Monitoring;

import java.util.function.Supplier;

public class Source<PACKET> extends AbstractSingleReceiverPublisher<PACKET> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private boolean exhausted;
    private FunctionalIterator<PACKET> iterator;

    public Source(Supplier<FunctionalIterator<PACKET>> iteratorSupplier, Monitoring monitor, String groupName) {
        super(monitor, groupName);
        this.iteratorSupplier = iteratorSupplier;
        this.exhausted = false;
    }

    public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier,
                                                             Monitoring monitor, String groupName) {
        return new Source<>(iteratorSupplier, monitor, groupName);
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        assert receiver.equals(receiverRegistry().receiver());
        assert !exhausted;
        if (iterator == null) iterator = iteratorSupplier.get();
        if (iterator.hasNext()) {
            monitor().onAnswerCreate(this);
            receiver.receive(this, iterator.next());
        } else {
            exhausted = true;
            monitor().onPathJoin(this);
        }
    }

}
