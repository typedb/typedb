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

public class Connector<OUTPUT_PID, PACKET> {

    private final Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController;
    private final Reactive.Identifier.Input<PACKET> inputId;
    private final List<Function<PACKET, PACKET>> transforms;
    private final OUTPUT_PID outputProcessorId;

    public Connector(Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController,
                     Controller.ConnectionRequest<?, OUTPUT_PID, PACKET, ?> connectionRequest) {
        this.providerController = providerController;
        this.inputId = connectionRequest.inputId();
        this.transforms = new ArrayList<>();
        this.outputProcessorId = connectionRequest.outputProcessorId();
    }

    public Connector(Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController,
                     Reactive.Identifier.Input<PACKET> inputId,
                     List<Function<PACKET, PACKET>> transforms, OUTPUT_PID outputProcessorId) {
        this.providerController = providerController;
        this.inputId = inputId;
        this.transforms = transforms;
        this.outputProcessorId = outputProcessorId;
    }

    public void applyTransforms(Reactive.Stream<PACKET, PACKET> toConnect, Processor.Output<PACKET> output) {
        Reactive.Provider.Sync.Publisher<PACKET> op = toConnect;
        for (Function<PACKET, PACKET> t : transforms) op = op.map(t);
        op.publishTo(output);
    }

    public Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> outputController() {
        return providerController;
    }

    public OUTPUT_PID outputProcessorId(){
        return outputProcessorId;
    }

    public Reactive.Identifier.Input<PACKET> inputId() {
        return inputId;
    }

    public Connector<OUTPUT_PID, PACKET> withMap(Function<PACKET, PACKET> function) {
        ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(transforms);
        newTransforms.add(function);
        return new Connector<>(providerController, inputId, newTransforms, outputProcessorId);
    }

    public Connector<OUTPUT_PID, PACKET> withNewProcessorId(OUTPUT_PID newPID) {
        return new Connector<>(providerController, inputId, transforms, newPID);
    }

}
