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
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

import java.util.function.Supplier;

public class Source<PACKET> extends SingleReceiverPublisher<PACKET> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private boolean exhausted;
    private FunctionalIterator<PACKET> iterator;

    public Source(Supplier<FunctionalIterator<PACKET>> iteratorSupplier, Processor<?, ?, ?, ?> processor) {
        super(processor);
        this.iteratorSupplier = iteratorSupplier;
        this.exhausted = false;
        processor().monitor().execute(actor -> actor.registerSource(identifier()));
    }

    public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<INPUT>> iteratorSupplier,
                                                             Processor<?, ?, ?, ?> processor) {
        return new Source<>(iteratorSupplier, processor);
    }

    @Override
    public void pull(Receiver.Sync<PACKET> receiver) {
        assert receiver.equals(receiverRegistry().receiver());
        receiverRegistry().recordPull(receiver);
        if (!exhausted) {
            if (iterator == null) iterator = iteratorSupplier.get();
            if (iterator.hasNext()) {
                processor().monitor().execute(actor -> actor.createAnswer(identifier()));
                receiver.receive(this, iterator.next());
            } else {
                exhausted = true;
                processor().monitor().execute(actor -> actor.sourceFinished(identifier()));
            }
        }
    }

}
