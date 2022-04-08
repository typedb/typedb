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

import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FanOutOperator<PACKET> implements Operator.Pool<PACKET, PACKET> {

    final Map<Reactive.Subscriber<PACKET>, Integer> bufferPositions;  // Points to the next item needed
    final Set<PACKET> bufferSet;
    final List<PACKET> bufferList;

    public FanOutOperator() {
        this.bufferSet = new HashSet<>();
        this.bufferList = new ArrayList<>();
        this.bufferPositions = new HashMap<>();
    }

    @Override
    public EffectsImpl accept(Reactive.Publisher<PACKET> publisher, PACKET packet) {
        EffectsImpl outcome = EffectsImpl.create();
        if (bufferSet.add(packet)) {
            bufferList.add(packet);
            outcome.addAnswerCreated();
        }
        outcome.addAnswerConsumed();
        return outcome;
    }

    @Override
    public boolean hasNext(Reactive.Subscriber<PACKET> subscriber) {
        bufferPositions.putIfAbsent(subscriber, 0);
        return bufferList.size() > bufferPositions.get(subscriber);
    }

    @Override
    public Supplied<PACKET> next(Reactive.Subscriber<PACKET> subscriber) {
        Integer pos = bufferPositions.get(subscriber);
        bufferPositions.put(subscriber, pos + 1);
        Supplied<PACKET> outcome = Supplied.create();
        outcome.setOutput(bufferList.get(pos));
        return outcome;
    }

}
