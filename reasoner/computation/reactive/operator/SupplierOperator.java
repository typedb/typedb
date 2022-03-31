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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.function.Supplier;

public class SupplierOperator<PACKET, RECEIVER> extends Operator.SourceImpl<Operator.Transformed<PACKET,?>, RECEIVER> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private boolean exhausted;
    private FunctionalIterator<PACKET> iterator;

    SupplierOperator(Supplier<FunctionalIterator<PACKET>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
        this.exhausted = false;
    }

    private FunctionalIterator<PACKET> iterator() {
        if (iterator == null) iterator = iteratorSupplier.get();
        return iterator;
    }

    @Override
    public boolean hasNext(RECEIVER receiver) {
        return !exhausted && iterator().hasNext();
    }

    @Override
    public Transformed<PACKET, ?> next(RECEIVER receiver) {
        Transformed<PACKET, ?> outcome = Transformed.create();
        if (!exhausted) {
            if (hasNext(receiver)) {
                outcome.addAnswerCreated();
                outcome.addOutput(iterator().next());
            } else {
                exhausted = true;
                outcome.addSourceFinished();
            }
        }
        return outcome;
    }

}
