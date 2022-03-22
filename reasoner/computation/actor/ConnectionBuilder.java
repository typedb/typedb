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

public class ConnectionBuilder<OUTPUT_PID, PACKET> {

    private final Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController;
    private final Reactive.Identifier.Input<PACKET> inputId;
    private final List<Function<PACKET, PACKET>> connectionTransforms;
    private final OUTPUT_PID providerProcessorId;

    public ConnectionBuilder(Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController,
                             Controller.ProviderRequest<?, OUTPUT_PID, PACKET, ?> providerRequest) {
        this.providerController = providerController;
        this.inputId = providerRequest.receiverInputId();
        this.connectionTransforms = providerRequest.connectionTransforms();
        this.providerProcessorId = providerRequest.providerProcessorId();
    }

    public ConnectionBuilder(Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController,
                             Reactive.Identifier.Input<PACKET> inputId,
                             List<Function<PACKET, PACKET>> connectionTransforms, OUTPUT_PID providerProcessorId) {
        this.providerController = providerController;
        this.inputId = inputId;
        this.connectionTransforms = connectionTransforms;
        this.providerProcessorId = providerProcessorId;
    }

    public List<Function<PACKET, PACKET>> connectionTransforms() {
        return connectionTransforms;
    }

    public Actor.Driver<? extends Controller<OUTPUT_PID, ?, PACKET, ?, ?>> providerController() {
        return providerController;
    }

    public OUTPUT_PID providerProcessorId(){
        return providerProcessorId;
    }

    public Reactive.Identifier.Input<PACKET> inputId() {
        return inputId;
    }

    public ConnectionBuilder<OUTPUT_PID, PACKET> withMap(Function<PACKET, PACKET> function) {
        ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(connectionTransforms);
        newTransforms.add(function);
        return new ConnectionBuilder<>(providerController, inputId, newTransforms, providerProcessorId);
    }

    public ConnectionBuilder<OUTPUT_PID, PACKET> withNewProcessorId(OUTPUT_PID newPID) {
        return new ConnectionBuilder<>(providerController, inputId, connectionTransforms, newPID);
    }

}
