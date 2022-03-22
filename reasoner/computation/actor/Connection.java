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

    private final Reactive.Identifier.Input<PACKET> recEndpointId;
    private final Reactive.Identifier.Output<PACKET> provEndpointId;
    private final List<Function<PACKET, PACKET>> transforms;

    Connection(Reactive.Identifier.Input<PACKET> recEndpointId, Reactive.Identifier.Output<PACKET> provEndpointId, List<Function<PACKET, PACKET>> transforms) {
        this.recEndpointId = recEndpointId;
        this.provEndpointId = provEndpointId;
        this.transforms = transforms;
    }

    protected Reactive.Identifier.Input<PACKET> receiverEndpointId() {
        return recEndpointId;
    }

    public Reactive.Identifier.Output<PACKET> providerEndpointId() {
        return provEndpointId;
    }

    public List<Function<PACKET, PACKET>> transformations() {
        return transforms;
    }

    public static class Builder<PROV_PID, PACKET> {

        private final Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> provController;
        private final Reactive.Identifier.Input<PACKET> recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PROV_PID provProcessorId;

        public Builder(Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> provController,
                       Controller.ProviderRequest<?, PROV_PID, PACKET, ?> providerRequest) {
            this.provController = provController;
            this.recEndpointId = providerRequest.receivingEndpointId();
            this.connectionTransforms = providerRequest.connectionTransforms();
            this.provProcessorId = providerRequest.providingProcessorId();
        }

        public Builder(Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> provController,
                       Reactive.Identifier.Input<PACKET> recEndpointId,
                       List<Function<PACKET, PACKET>> connectionTransforms, PROV_PID provProcessorId) {
            this.provController = provController;
            this.recEndpointId = recEndpointId;
            this.connectionTransforms = connectionTransforms;
            this.provProcessorId = provProcessorId;
        }

        public Actor.Driver<? extends Controller<PROV_PID, ?, PACKET, ?, ?>> providerController() {
            return provController;
        }

        public PROV_PID providerProcessorId(){
            return provProcessorId;
        }

        public Reactive.Identifier.Input<PACKET> receiverEndpointId() {
            return recEndpointId;
        }

        public Builder<PROV_PID, PACKET> withMap(Function<PACKET, PACKET> function) {
            ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(connectionTransforms);
            newTransforms.add(function);
            return new Builder<>(provController, recEndpointId, newTransforms, provProcessorId);
        }

        public Builder<PROV_PID, PACKET> withNewProcessorId(PROV_PID newPID) {
            return new Builder<>(provController, recEndpointId, connectionTransforms, newPID);
        }

        public Connection<PACKET> build(Reactive.Identifier.Output<PACKET> pubEndpointId) {
            return new Connection<>(recEndpointId, pubEndpointId, connectionTransforms);
        }
    }
}
