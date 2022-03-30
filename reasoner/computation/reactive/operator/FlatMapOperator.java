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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider;

import java.util.function.Function;

public class FlatMapOperator<INPUT, OUTPUT, PROVIDER extends Provider<?>> implements Operator<INPUT, OUTPUT, PROVIDER> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;

    public FlatMapOperator(Function<INPUT, FunctionalIterator<OUTPUT>> transform) {
        this.transform = transform;
    }

    @Override
    public Outcome<OUTPUT, PROVIDER> operate(PROVIDER provider, INPUT packet) {
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        // This can actually create more receive() calls to downstream than the number of pulls it receives. Protect
        // against by manually adding .buffer() after calls to flatMap
        Outcome<OUTPUT, PROVIDER> outcome = Outcome.create();
        transformed.forEachRemaining(t -> {
            outcome.addAnswerCreated();
            outcome.addOutput(t);
        });
        outcome.addAnswerConsumed();
        return outcome;
    }

}
