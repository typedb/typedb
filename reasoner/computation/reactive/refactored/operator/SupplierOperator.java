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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator.Supplied;

import java.util.function.Supplier;

// TODO: This is now such a thin wrapper it could be collapsed
public class SupplierOperator<PACKET, RECEIVER> implements Operator.Source<PACKET, RECEIVER> {

    private final Supplier<FunctionalIterator<PACKET>> iteratorSupplier;
    private FunctionalIterator<PACKET> iterator;

    public SupplierOperator(Supplier<FunctionalIterator<PACKET>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    private FunctionalIterator<PACKET> iterator() {
        if (iterator == null) iterator = iteratorSupplier.get();
        return iterator;
    }

    @Override
    public boolean isExhausted(RECEIVER receiver) {
        return !iterator().hasNext();
    }

    @Override
    public Supplied<PACKET, Void> next(RECEIVER receiver) {
        Supplied<PACKET, Void> outcome = Supplied.create();
        assert !isExhausted(receiver);
        outcome.addAnswerCreated();
        outcome.setOutput(iterator().next());
        return outcome;
    }

}
