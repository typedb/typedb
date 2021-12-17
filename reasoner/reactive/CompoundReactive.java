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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class CompoundReactive<PLAN_ID, PACKET> extends IdentityReactive<PACKET> {

    private final Publisher<PACKET> leadingPublisher;
    private final List<PLAN_ID> remainingPlan;
    private final BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc;
    private final BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc;
    private final Map<Provider<PACKET>, PACKET> publisherPackets;

    public CompoundReactive(List<PLAN_ID> plan, BiFunction<PLAN_ID, PACKET, Publisher<PACKET>> spawnLeaderFunc,
                            BiFunction<PACKET, PACKET, PACKET> compoundPacketsFunc, PACKET initialPacket) {
        super(new HashSet<>());
        assert plan.size() > 0;
        this.remainingPlan = new ArrayList<>(plan);
        this.leadingPublisher = spawnLeaderFunc.apply(this.remainingPlan.remove(0), initialPacket);
        this.compoundPacketsFunc = compoundPacketsFunc;
        this.spawnLeaderFunc = spawnLeaderFunc;
        this.publisherPackets = new HashMap<>();
        this.leadingPublisher.publishTo(this);
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        // TODO: Need to implement equals and hashCode? Seems we actually want exact object matches only for reactives
        if (leadingPublisher.equals(provider)) {
            // TODO: Should we pull again from the leader?
            if (remainingPlan.size() == 0) {  // For a single item plan
                finishPulling();
                subscriber().receive(this, packet);
            } else {
                Publisher<PACKET> nextPublisher;
                if (remainingPlan.size() == 1) nextPublisher = spawnLeaderFunc.apply(remainingPlan.remove(0), packet);
                else nextPublisher = new CompoundReactive<>(remainingPlan, spawnLeaderFunc, compoundPacketsFunc, packet);
                publisherPackets.put(nextPublisher, packet);
                nextPublisher.publishTo(this);
                nextPublisher.pull(this);
            }
        } else {
            PACKET compoundedPacket = compoundPacketsFunc.apply(packet, publisherPackets.get(provider));
            super.receive(this, compoundedPacket);
        }
    }

}
