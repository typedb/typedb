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

import java.util.List;
import java.util.function.Function;

public class Connection<PACKET, PROCESSOR extends Processor<PACKET, ?, PROCESSOR>, PUB_PROCESSOR extends Processor<?, PACKET, PUB_PROCESSOR>> {

    private final Actor.Driver<PROCESSOR> recProcessor;
    private final Actor.Driver<PUB_PROCESSOR> provProcessor;
    private final long recEndpointId;
    private final long provEndpointId;
    private final List<Function<PACKET, PACKET>> transforms;

    /**
     * Connects a processor outlet (upstream, publishing) to another processor's inlet (downstream, subscribing)
     */
    Connection(Actor.Driver<PROCESSOR> recProcessor, Actor.Driver<PUB_PROCESSOR> provProcessor, long recEndpointId,
               long provEndpointId,
               List<Function<PACKET, PACKET>> transforms) {
        this.recProcessor = recProcessor;
        this.provProcessor = provProcessor;
        this.recEndpointId = recEndpointId;
        this.provEndpointId = provEndpointId;
        this.transforms = transforms;
    }

    protected void receive(PACKET packet) {
        recProcessor.execute(actor -> actor.endpointReceive(packet, recEndpointId));
    }

    protected void pull() {
        provProcessor.execute(actor -> actor.endpointPull(provEndpointId));
    }

    protected long subEndpointId() {
        return recEndpointId;
    }

    public long providingEndpointId() {
        return provEndpointId;
    }

    public List<Function<PACKET, PACKET>> transformations() {
        return transforms;
    }

    public static class Builder<PUB_PROC_ID, PACKET,
            REQ extends Processor.Request<?, PUB_PROC_ID, PUB_CONTROLLER, PACKET, PROCESSOR, REQ>,  // TODO: Try removing REQ
            PROCESSOR extends Processor<PACKET, ?, PROCESSOR>,
            PUB_CONTROLLER extends Controller<PUB_PROC_ID, ?, PACKET, ?, PUB_CONTROLLER>> {

        private final Actor.Driver<PUB_CONTROLLER> provController;
        private final Processor.Request<?, PUB_PROC_ID, PUB_CONTROLLER, PACKET, PROCESSOR, REQ> connectionRequest;

        public Builder(Actor.Driver<PUB_CONTROLLER> provController, Processor.Request<?, PUB_PROC_ID, PUB_CONTROLLER, PACKET, PROCESSOR, REQ> connectionRequest) {
            this.provController = provController;
            this.connectionRequest = connectionRequest;
        }

        public Actor.Driver<PUB_CONTROLLER> providerController() {
            return provController;
        }

        public Processor.Request<?, PUB_PROC_ID, PUB_CONTROLLER, PACKET, PROCESSOR, REQ> request() {
            return connectionRequest;
        }

        public Builder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, PUB_CONTROLLER> withMap(Function<PACKET, PACKET> function) {
            connectionRequest.withMap(function);
            return this;
        }

        public Builder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, PUB_CONTROLLER> withNewProcessorId(PUB_PROC_ID newPID) {
            connectionRequest.withNewProcessorId(newPID);
            return this;
        }

        public <PUB_PROCESSOR extends Processor<?, PACKET, PUB_PROCESSOR>> Connection<PACKET, PROCESSOR, PUB_PROCESSOR> build(Actor.Driver<PUB_PROCESSOR> pubProcessor, long pubEndpointId) {
            return new Connection<>(request().recProcessor(), pubProcessor, request().recEndpointId(), pubEndpointId, request().connectionTransforms());
        }
    }
}
