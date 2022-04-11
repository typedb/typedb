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
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.TransformationStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.vaticle.typedb.common.collection.Collections.set;

public class CompoundOperator<PLAN_ID, PACKET> implements Operator.Transformer<PACKET, PACKET> {

    private final Publisher<PACKET> leadingPublisher;
    private final List<PLAN_ID> remainingPlan;
    private final BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc;
    private final BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc;
    private final Map<Publisher<PACKET>, PACKET> publisherPackets;
    private final PACKET initialPacket;
    private final Processor<?, ?, ?, ?> processor;

    public CompoundOperator(Processor<?, ?, ?, ?> processor, List<PLAN_ID> plan,
                            BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc,
                            BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc, PACKET initialPacket) {
        this.processor = processor;
        assert plan.size() > 0;
        this.initialPacket = initialPacket;
        this.remainingPlan = new ArrayList<>(plan);
        this.compoundPacketsFunc = compoundPacketsFunc;
        this.spawnLeaderFunc = spawnLeaderFunc;
        this.publisherPackets = new HashMap<>();
        this.leadingPublisher = spawnLeaderFunc.apply(this.remainingPlan.remove(0), initialPacket);
    }

    @Override
    public Set<Publisher<PACKET>> initialise() {
        return set(this.leadingPublisher);
    }

    @Override
    public Either<Publisher<PACKET>, Set<PACKET>> accept(Publisher<PACKET> publisher, PACKET packet) {
        PACKET mergedPacket = compoundPacketsFunc.apply(initialPacket, packet);
        if (leadingPublisher.equals(publisher)) {
            if (remainingPlan.size() == 0) {  // For a single item plan
                return Either.second(set(mergedPacket));
            } else {
                Publisher<PACKET> follower;  // TODO: Creation of a new publisher should be delegated to the owner of this operation
                if (remainingPlan.size() == 1) {
                    follower = spawnLeaderFunc.apply(remainingPlan.get(0), mergedPacket);
                } else {
                    follower = TransformationStream.fanIn(
                            processor,
                            new CompoundOperator<>(processor, remainingPlan, spawnLeaderFunc, compoundPacketsFunc,
                                                   mergedPacket)
                    ).buffer();
                }
                publisherPackets.put(follower, mergedPacket);
                return Either.first(follower);
            }
        } else {
            PACKET compoundedPacket = compoundPacketsFunc.apply(mergedPacket, publisherPackets.get(publisher));
            return Either.second(set(compoundedPacket));
        }
    }

}
