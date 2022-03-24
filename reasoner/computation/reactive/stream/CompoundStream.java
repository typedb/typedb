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

package com.vaticle.typedb.core.reasoner.computation.reactive.stream;


import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class CompoundStream<PLAN_ID, PACKET> extends SingleReceiverMultiProviderStream<PACKET, PACKET> {

    private final Publisher<PACKET> leadingPublisher;
    private final List<PLAN_ID> remainingPlan;
    private final BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc;
    private final BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc;
    private final Map<Publisher<PACKET>, PACKET> publisherPackets;
    private final PACKET initialPacket;

    public CompoundStream(List<PLAN_ID> plan, BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc,
                          BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc, PACKET initialPacket,
                          Processor<?, ?, ?, ?> processor) {  // TODO: Processor should always be the first argument since it's the owner
        super(processor);
        assert plan.size() > 0;
        this.initialPacket = initialPacket;
        this.remainingPlan = new ArrayList<>(plan);
        this.leadingPublisher = spawnLeaderFunc.apply(this.remainingPlan.remove(0), initialPacket);
        this.compoundPacketsFunc = compoundPacketsFunc;
        this.spawnLeaderFunc = spawnLeaderFunc;
        this.publisherPackets = new HashMap<>();
        this.leadingPublisher.registerSubscriber(this);
    }

    @Override
    public void receive(Publisher<PACKET> publisher, PACKET packet) {
        super.receive(publisher, packet);
        PACKET mergedPacket = compoundPacketsFunc.apply(initialPacket, packet);
        if (leadingPublisher.equals(publisher)) {
            if (remainingPlan.size() == 0) {  // For a single item plan
                receiverRegistry().setNotPulling();
                receiverRegistry().receiver().receive(this, mergedPacket);
            } else {
                Publisher<PACKET> follower;
                if (remainingPlan.size() == 1) {
                    follower = spawnLeaderFunc.apply(remainingPlan.get(0), mergedPacket);
                } else {
                    follower = new CompoundStream<>(remainingPlan, spawnLeaderFunc, compoundPacketsFunc, mergedPacket, processor()).buffer();
                }
                publisherPackets.put(follower, mergedPacket);
                processor().monitor().execute(actor -> actor.forkFrontier(1, identifier()));
                processor().monitor().execute(actor -> actor.consumeAnswer(identifier()));
                follower.registerSubscriber(this);
                if (receiverRegistry().isPulling()) {
                    processor().schedulePullRetry(leadingPublisher, this);  // Retry the leader in case the follower never produces an answer
                    if (providerRegistry().setPulling(follower)) follower.pull(this);
                }
            }
        } else {
            receiverRegistry().setNotPulling();
            PACKET compoundedPacket = compoundPacketsFunc.apply(mergedPacket, publisherPackets.get(publisher));
            receiverRegistry().receiver().receive(this, compoundedPacket);
        }
    }
}
