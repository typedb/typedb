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

package com.vaticle.typedb.core.reasoner.computation.reactive.operator;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.AbstractStream;

import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class SupplierOperator<PACKET> implements AbstractStream.SimpleBuffer<Operator.Outcome<PACKET,?>> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private boolean exhausted;
    private FunctionalIterator<PACKET> iterator;

    SupplierOperator(Supplier<FunctionalIterator<PACKET>> iteratorSupplier, Processor<?, ?, ?, ?> processor) {
        this.iteratorSupplier = iteratorSupplier;
        this.exhausted = false;
        // processor.monitor().execute(actor -> actor.registerSource(identifier()));  // TODO: How do we register a source? We can't return an Outcome object without a separate initialise method
    }

    private FunctionalIterator<PACKET> iterator() {
        if (iterator == null) iterator = iteratorSupplier.get();
        return iterator;
    }

    @Override
    public boolean hasNext() {
        return !exhausted && iterator().hasNext();
    }

    @Override
    public Operator.Outcome<PACKET, ?> next() {
        Operator.Outcome<PACKET, ?> outcome = Operator.Outcome.create();
        if (!exhausted) {
            if (hasNext()) {
                outcome.addAnswerCreated();
                outcome.addOutput(iterator().next());
            } else {
                exhausted = true;
                outcome.addSourceFinished();
            }
        }
        return outcome;
    }

    @Override
    public void add(Set<Operator.Outcome<PACKET, ?>> packets) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }
}
