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
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator.Transformed;

import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public class FlatMapOperator<INPUT, OUTPUT> implements Operator.Transformer<INPUT, OUTPUT> {

    private final Function<INPUT, FunctionalIterator<OUTPUT>> transform;

    public FlatMapOperator(Function<INPUT, FunctionalIterator<OUTPUT>> transform) {
        this.transform = transform;
    }

    @Override
    public Set<Publisher<INPUT>> initialise() {
        return set();
    }

    @Override
    public Transformed<OUTPUT, INPUT> accept(Publisher<INPUT> publisher, INPUT packet) {
        FunctionalIterator<OUTPUT> transformed = transform.apply(packet);
        // This can actually create more receive() calls to downstream than the number of pulls it receives. Protect
        // against by manually adding .buffer() after calls to flatMap
        Transformed<OUTPUT, INPUT> outcome = Transformed.create();
        transformed.forEachRemaining(t -> {
            outcome.addAnswerCreated();
            outcome.addOutput(t);
        });
        outcome.addAnswerConsumed();
        return outcome;
    }

}
