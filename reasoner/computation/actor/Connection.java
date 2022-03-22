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

public class Connection<PACKET> {

    private final Reactive.Identifier.Input<PACKET> receiverInputId;
    private final Reactive.Identifier.Output<PACKET> providerOutputId;
    private final List<Function<PACKET, PACKET>> transforms;

    Connection(Reactive.Identifier.Input<PACKET> receiverInputId, Reactive.Identifier.Output<PACKET> providerOutputId,
               List<Function<PACKET, PACKET>> transforms) {
        this.receiverInputId = receiverInputId;
        this.providerOutputId = providerOutputId;
        this.transforms = transforms;
    }

    protected Reactive.Identifier.Input<PACKET> receiverEndpointId() {
        return receiverInputId;
    }

    public Reactive.Identifier.Output<PACKET> providerEndpointId() {
        return providerOutputId;
    }

    public List<Function<PACKET, PACKET>> transformations() {
        return transforms;
    }

    public static class Builder<PROV_PID, PACKET> {

        private final Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> providerController;
        private final Reactive.Identifier.Input<PACKET> receicerInputId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PROV_PID providerProcessorId;

        public Builder(Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> providerController,
                       Controller.ProviderRequest<?, PROV_PID, PACKET, ?> providerRequest) {
            this.providerController = providerController;
            this.receicerInputId = providerRequest.receiverInputId();
            this.connectionTransforms = providerRequest.connectionTransforms();
            this.providerProcessorId = providerRequest.providerProcessorId();
        }

        public Builder(Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> providerController,
                       Reactive.Identifier.Input<PACKET> receicerInputId,
                       List<Function<PACKET, PACKET>> connectionTransforms, PROV_PID providerProcessorId) {
            this.providerController = providerController;
            this.receicerInputId = receicerInputId;
            this.connectionTransforms = connectionTransforms;
            this.providerProcessorId = providerProcessorId;
        }

        public Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> providerController() {
            return providerController;
        }

        public PROV_PID providerProcessorId(){
            return providerProcessorId;
        }

        public Reactive.Identifier.Input<PACKET> receiverInputId() {
            return receicerInputId;
        }

        public Builder<PROV_PID, PACKET> withMap(Function<PACKET, PACKET> function) {
            ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(connectionTransforms);
            newTransforms.add(function);
            return new Builder<>(providerController, receicerInputId, newTransforms, providerProcessorId);
        }

        public Builder<PROV_PID, PACKET> withNewProcessorId(PROV_PID newPID) {
            return new Builder<>(providerController, receicerInputId, connectionTransforms, newPID);
        }

        public Connection<PACKET> build(Reactive.Identifier.Output<PACKET> pubEndpointId) {
            return new Connection<>(receicerInputId, pubEndpointId, connectionTransforms);
        }
    }
}
