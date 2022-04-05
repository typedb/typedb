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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class CompoundOperator<PLAN_ID, PACKET, RECEIVER> implements Operator.Transformer<PACKET, PACKET, Publisher<PACKET>, RECEIVER>, Operator<PACKET, PACKET, Publisher<PACKET>, RECEIVER> {

    private final Publisher<PACKET> leadingPublisher;
    private final List<PLAN_ID> remainingPlan;
    private final BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc;
    private final BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc;
    private final Map<Publisher<PACKET>, PACKET> publisherPackets;
    private final PACKET initialPacket;
    private final Processor<?, ?, ?, ?> processor;

    CompoundOperator(Processor<?, ?, ?, ?> processor, List<PLAN_ID> plan,
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
        // this.leadingPublisher.registerReceiver(this);  // TODO: This requires creating a new op + stream on construction. Perhaps this suggests this model of resolving compounds needs to change
    }

    @Override
    public Transformed<PACKET, Publisher<PACKET>> accept(Publisher<PACKET> provider, PACKET packet) {
        PACKET mergedPacket = compoundPacketsFunc.apply(initialPacket, packet);
        Transformed<PACKET, Publisher<PACKET>> outcome = Transformed.create();
        if (leadingPublisher.equals(provider)) {
            if (remainingPlan.size() == 0) {  // For a single item plan
                outcome.addOutput(mergedPacket);
            } else {
                Publisher<PACKET> follower;
                if (remainingPlan.size() == 1) {
                    follower = spawnLeaderFunc.apply(remainingPlan.get(0), mergedPacket);
                } else {
//                    follower = AbstractStream.SyncStream.simple(
//                            processor,
//                            new CompoundOperator<>(processor, remainingPlan, spawnLeaderFunc, compoundPacketsFunc,
//                                                   initialPacket)
//                    ).buffer();
                    follower = null;
                    outcome.addNewProvider(follower);
                }
                publisherPackets.put(follower, mergedPacket);
                outcome.addAnswerConsumed();
            }
        } else {
            PACKET compoundedPacket = compoundPacketsFunc.apply(mergedPacket, publisherPackets.get(provider));
            outcome.addOutput(compoundedPacket);
        }
        return outcome;
    }

}
