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

package com.vaticle.typedb.core.reasoner.reactive;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static com.vaticle.typedb.common.collection.Collections.set;

public class CompoundReactive<PLAN_ID, PACKET> extends IdentityReactive<PACKET> {

    private final Provider<PACKET> leadingPublisher;
    private final PLAN_ID nextPlanElement;
    private final List<PLAN_ID> remainingPlan;
    private final BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc;
    private final BiFunction<PLAN_ID, PACKET, Provider<PACKET>> spawnLeaderFunc;
    private final Map<Provider<PACKET>, PACKET> publisherPackets;

    public CompoundReactive(Provider<PACKET> leadingPublisher,
                            List<PLAN_ID> plan, BiFunction<PLAN_ID, PACKET, Provider<PACKET>> spawnLeaderFunc,
                            BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc) {
        super(set(leadingPublisher));
        this.leadingPublisher = leadingPublisher;
        this.remainingPlan = new ArrayList<>(plan);
        this.compoundPacketsFunc = compoundPacketsFunc;
        this.nextPlanElement = this.remainingPlan.remove(0);
        this.spawnLeaderFunc = spawnLeaderFunc;
        this.publisherPackets = new HashMap<>();
    }

    public static <P, T> CompoundReactive<P, T> compound(List<P> plan, T initialPacket,
                                                         BiFunction<P, T, Provider<T>> spawnLeaderFunc,
                                                         BiFunction<T, T, T> compoundPacketsFunc) {
        List<P> remainingPlan = new ArrayList<>(plan);
        P firstPlanElement = remainingPlan.remove(0);
        Provider<T> lead = spawnLeaderFunc.apply(firstPlanElement, initialPacket);
        return new CompoundReactive<>(lead, remainingPlan, spawnLeaderFunc, compoundPacketsFunc);
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        // TODO: Need to implement equals and hashCode? Seems we actually want exact object matches only for reactives
        if (leadingPublisher.equals(provider)) {
            // TODO: Should we pull again from the leader?
            Provider<PACKET> nextLeader = spawnLeaderFunc.apply(nextPlanElement, packet);
            Provider<PACKET> nextPublisher;
            if (remainingPlan.size() == 0) nextPublisher = nextLeader;
            else nextPublisher = new CompoundReactive<>(nextLeader, remainingPlan, spawnLeaderFunc, compoundPacketsFunc);
            publisherPackets.put(nextPublisher, packet);
            subscribeTo(nextPublisher);
        } else {
            PACKET compoundedPacket = compoundPacketsFunc.apply(packet, publisherPackets.get(provider));
            super.receive(this, compoundedPacket);
        }
    }

}
