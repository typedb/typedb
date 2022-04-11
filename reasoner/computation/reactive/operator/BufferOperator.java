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

import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator.Supplied;

import java.util.Stack;

public class BufferOperator<PACKET> implements Operator.Pool<PACKET, PACKET> {

    private final Stack<PACKET> stack;

    public BufferOperator() {
        this.stack = new Stack<>();
    }

    @Override
    public boolean accept(Reactive.Publisher<PACKET> publisher, PACKET packet) {
        stack.add(packet);
        return true;
    }

    @Override
    public boolean hasNext(Reactive.Subscriber<PACKET> subscriber) {
        return stack.size() > 0;
    }

    @Override
    public Supplied<PACKET> next(Reactive.Subscriber<PACKET> subscriber) {
        Supplied<PACKET> outcome = Supplied.create();
        outcome.setOutput(stack.pop());
        return outcome;
    }

}
