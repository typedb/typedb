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

import java.util.HashSet;
import java.util.Set;

public class DistinctOperator<PACKET, PROVIDER> implements Operator<PACKET, PACKET, PROVIDER> {

    private final Set<PACKET> deduplicationSet;

    DistinctOperator() {
        this.deduplicationSet = new HashSet<>();
    }

    @Override
    public Outcome<PACKET, PROVIDER> operate(PROVIDER provider, PACKET packet) {
        Outcome<PACKET, PROVIDER> outcome = Outcome.create();
        if (deduplicationSet.add(packet)) {
            outcome.addOutput(packet);
        } else {
            outcome.addAnswerConsumed();
        }
        return outcome;
    }
}
