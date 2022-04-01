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

public class SupplierOperator<PACKET, RECEIVER> implements Operator.Source<PACKET, RECEIVER>, Operator<Void, PACKET, Void, RECEIVER> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private boolean exhausted;
    private FunctionalIterator<PACKET> iterator;

    public SupplierOperator(Supplier<FunctionalIterator<PACKET>> iteratorSupplier) {
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
    public Supplied<PACKET, Void> next(RECEIVER receiver) {
        Supplied<PACKET, Void> outcome = Supplied.create();
        if (!exhausted) {
            if (hasNext(receiver)) {
                outcome.addAnswerCreated();
                outcome.setOutput(iterator().next());
            } else {
                exhausted = true;
                outcome.addSourceFinished();
            }
        }
        return outcome;
    }

}
