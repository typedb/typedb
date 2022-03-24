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

public class Connector<BOUNDS, PACKET> {

    private final Actor.Driver<? extends Controller<BOUNDS, ?, PACKET, ?, ?>> upstreamController;
    private final Reactive.Identifier<PACKET, ?> inputId;
    private final List<Function<PACKET, PACKET>> transforms;
    private final BOUNDS bounds;

    public Connector(Actor.Driver<? extends Controller<BOUNDS, ?, PACKET, ?, ?>> upstreamController,
                     Controller.ConnectionRequest<?, BOUNDS, PACKET, ?> connectionRequest) {
        this.upstreamController = upstreamController;
        this.inputId = connectionRequest.inputId();
        this.transforms = new ArrayList<>();
        this.bounds = connectionRequest.bounds();
    }

    public Connector(Actor.Driver<? extends Controller<BOUNDS, ?, PACKET, ?, ?>> upstreamController,
                     Reactive.Identifier<PACKET, ?> inputId, List<Function<PACKET, PACKET>> transforms,
                     BOUNDS bounds) {
        this.upstreamController = upstreamController;
        this.inputId = inputId;
        this.transforms = transforms;
        this.bounds = bounds;
    }

    public void connectViaTransforms(Reactive.Stream<PACKET, PACKET> toConnect, Processor.Output<PACKET> output) {
        Reactive.Publisher<PACKET> op = toConnect;
        for (Function<PACKET, PACKET> t : transforms) op = op.map(t);
        op.registerSubscriber(output);
    }

    public Actor.Driver<? extends Controller<BOUNDS, ?, PACKET, ?, ?>> upstreamController() {
        return upstreamController;
    }

    public BOUNDS bounds(){
        return bounds;
    }

    public Reactive.Identifier<PACKET, ?> inputId() {
        return inputId;
    }

    public Connector<BOUNDS, PACKET> withMap(Function<PACKET, PACKET> function) {
        ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(transforms);
        newTransforms.add(function);
        return new Connector<>(upstreamController, inputId, newTransforms, bounds);
    }

    public Connector<BOUNDS, PACKET> withNewBounds(BOUNDS newBounds) {
        return new Connector<>(upstreamController, inputId, transforms, newBounds);
    }

}
