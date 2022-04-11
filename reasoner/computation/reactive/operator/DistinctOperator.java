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

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;

public class DistinctOperator<PACKET> implements Operator.Transformer<PACKET, PACKET> {

    private final Set<PACKET> deduplicationSet;

    public DistinctOperator() {
        this.deduplicationSet = new HashSet<>();
    }

    @Override
    public Set<Publisher<PACKET>> initialNewPublishers() {
        return set();
    }

    @Override
    public Either<Publisher<PACKET>, Set<PACKET>> accept(Publisher<PACKET> publisher, PACKET packet) {
        if (deduplicationSet.add(packet)) return Either.second(set(packet));
        else return Either.second(set());
    }
}
