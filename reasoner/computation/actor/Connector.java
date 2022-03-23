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

package com.vaticle.typedb.core.reasoner.computation.actor;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Connector<UPSTREAM_PID, PACKET> {

    private final Actor.Driver<? extends Controller<UPSTREAM_PID, ?, PACKET, ?, ?>> upstreamController;
    private final Reactive.Identifier.Input<PACKET> inputId;
    private final List<Function<PACKET, PACKET>> transforms;
    private final UPSTREAM_PID upstreamProcessorId;

    public Connector(Actor.Driver<? extends Controller<UPSTREAM_PID, ?, PACKET, ?, ?>> upstreamController,
                     Controller.ConnectionRequest<?, UPSTREAM_PID, PACKET, ?> connectionRequest) {
        this.upstreamController = upstreamController;
        this.inputId = connectionRequest.inputId();
        this.transforms = new ArrayList<>();
        this.upstreamProcessorId = connectionRequest.upstreamProcessorId();
    }

    public Connector(Actor.Driver<? extends Controller<UPSTREAM_PID, ?, PACKET, ?, ?>> upstreamController,
                     Reactive.Identifier.Input<PACKET> inputId, List<Function<PACKET, PACKET>> transforms,
                     UPSTREAM_PID upstreamProcessorId) {
        this.upstreamController = upstreamController;
        this.inputId = inputId;
        this.transforms = transforms;
        this.upstreamProcessorId = upstreamProcessorId;
    }

    public void connectViaTransforms(Reactive.Stream<PACKET, PACKET> toConnect, Processor.Output<PACKET> output) {
        Reactive.Provider.Sync.Publisher<PACKET> op = toConnect;
        for (Function<PACKET, PACKET> t : transforms) op = op.map(t);
        op.publishTo(output);
    }

    public Actor.Driver<? extends Controller<UPSTREAM_PID, ?, PACKET, ?, ?>> upstreamController() {
        return upstreamController;
    }

    public UPSTREAM_PID upstreamProcessorId(){
        return upstreamProcessorId;
    }

    public Reactive.Identifier.Input<PACKET> inputId() {
        return inputId;
    }

    public Connector<UPSTREAM_PID, PACKET> withMap(Function<PACKET, PACKET> function) {
        ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(transforms);
        newTransforms.add(function);
        return new Connector<>(upstreamController, inputId, newTransforms, upstreamProcessorId);
    }

    public Connector<UPSTREAM_PID, PACKET> withNewProcessorId(UPSTREAM_PID newPID) {
        return new Connector<>(upstreamController, inputId, transforms, newPID);
    }

}
