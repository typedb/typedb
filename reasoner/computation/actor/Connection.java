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
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class Connection<PACKET, PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
        PROV_PROCESSOR extends Processor<?, PACKET, ?, PROV_PROCESSOR>>
        implements Reactive.Provider<PACKET>, Reactive.Receiver<PACKET> {

    private final Identifier identifier;
    private final Actor.Driver<PROCESSOR> recProcessor;
    private final Actor.Driver<PROV_PROCESSOR> provProcessor;
    private final long recEndpointId;
    private final long provEndpointId;
    private final List<Function<PACKET, PACKET>> transforms;
    private final Supplier<String> tracingGroupName;

    /**
     * Connects a processor outlet (upstream, publishing) to another processor's inlet (downstream, subscribing)
     */
    Connection(Actor.Driver<PROCESSOR> recProcessor, Actor.Driver<PROV_PROCESSOR> provProcessor, long recEndpointId,
               long provEndpointId, List<Function<PACKET, PACKET>> transforms) {
        this.identifier = new Identifier(recProcessor, provProcessor, recEndpointId, provEndpointId);
        this.recProcessor = recProcessor;
        this.provProcessor = provProcessor;
        this.recEndpointId = recEndpointId;
        this.provEndpointId = provEndpointId;
        this.transforms = transforms;
        this.tracingGroupName = () -> Connection.class.getSimpleName() + "@" + System.identityHashCode(this);
    }

    @Override
    public Supplier<String> tracingGroupName() {
        return tracingGroupName;
    }

    @Override
    public Connection.Identifier identifier() {
        return identifier;
    }

    @Override
    public void receive(Provider<PACKET> provider, PACKET packet) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.receive(provider, this, packet));
        recProcessor.execute(actor -> actor.endpointReceive(this, packet, recEndpointId));
    }

    @Override
    public void pull(Receiver<PACKET> receiver) {
        provProcessor.execute(actor -> actor.endpointPull(this, provEndpointId));
    }

    protected long receiverEndpointId() {
        return recEndpointId;
    }

    public long providerEndpointId() {
        return provEndpointId;
    }

    public List<Function<PACKET, PACKET>> transformations() {
        return transforms;
    }

    public static class Identifier implements Reactive.Identifier {

        private final Actor.Driver<? extends Processor<?, ?, ?, ?>> recProcessor;
        private final Actor.Driver<? extends Processor<?, ?, ?, ?>> provProcessor;
        private final long recEndpointId;
        private final long provEndpointId;

        public Identifier(Actor.Driver<? extends Processor<?, ?, ?, ?>> recProcessor,
                          Actor.Driver<? extends Processor<?, ?, ?, ?>> provProcessor, long recEndpointId,
                          long provEndpointId) {
            super();

            this.recProcessor = recProcessor;
            this.provProcessor = provProcessor;
            this.recEndpointId = recEndpointId;
            this.provEndpointId = provEndpointId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Identifier that = (Identifier) o;
            return recEndpointId == that.recEndpointId &&
                    provEndpointId == that.provEndpointId &&
                    recProcessor.equals(that.recProcessor) &&
                    provProcessor.equals(that.provProcessor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recProcessor, provProcessor, recEndpointId, provEndpointId);
        }

        @Override
        public String toString() {
            return recProcessor.debugName().get() + ":" + recEndpointId + "<->" + provProcessor.debugName().get() +
                    ":" + provEndpointId;
        }
    }

    public static class Builder<PROV_PID, PACKET,
            REQ extends Controller.ProviderRequest<?, PROV_PID, PROV_CONTROLLER, PACKET, PROCESSOR, ?, REQ>,
            PROCESSOR extends Processor<PACKET, ?, ?, PROCESSOR>,
            PROV_CONTROLLER extends Controller<PROV_PID, ?, PACKET, ?, PROV_CONTROLLER>> {

        private final Actor.Driver<PROV_CONTROLLER> provController;
        private final Actor.Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<PACKET, PACKET>> connectionTransforms;
        private final PROV_PID provProcessorId;

        public Builder(Actor.Driver<PROV_CONTROLLER> provController,
                       Controller.ProviderRequest<?, PROV_PID, PROV_CONTROLLER, PACKET, PROCESSOR, ?, REQ> providerRequest) {
            this.provController = provController;
            this.recProcessor = providerRequest.receivingProcessor();
            this.recEndpointId = providerRequest.receivingEndpointId();
            this.connectionTransforms = providerRequest.connectionTransforms();
            this.provProcessorId = providerRequest.providingProcessorId();
        }

        public Builder(Actor.Driver<PROV_CONTROLLER> provController, Actor.Driver<PROCESSOR> recProcessor, long recEndpointId,
                       List<Function<PACKET, PACKET>> connectionTransforms, PROV_PID provProcessorId) {
            this.provController = provController;
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.connectionTransforms = connectionTransforms;
            this.provProcessorId = provProcessorId;
        }

        public Actor.Driver<PROV_CONTROLLER> providerController() {
            return provController;
        }

        public PROV_PID providerProcessorId(){
            return provProcessorId;
        }

        public Actor.Driver<PROCESSOR> receivingProcessor() {
            return recProcessor;
        }

        public Builder<PROV_PID, PACKET, REQ, PROCESSOR, PROV_CONTROLLER> withMap(Function<PACKET, PACKET> function) {
            ArrayList<Function<PACKET, PACKET>> newTransforms = new ArrayList<>(connectionTransforms);
            newTransforms.add(function);
            return new Builder<>(provController, recProcessor, recEndpointId, newTransforms, provProcessorId);
        }

        public Builder<PROV_PID, PACKET, REQ, PROCESSOR, PROV_CONTROLLER> withNewProcessorId(PROV_PID newPID) {
            return new Builder<>(provController, recProcessor, recEndpointId, connectionTransforms, newPID);
        }

        public <PROV_PROCESSOR extends Processor<?, PACKET, ?, PROV_PROCESSOR>> Connection<PACKET, PROCESSOR,
                PROV_PROCESSOR> build(Actor.Driver<PROV_PROCESSOR> pubProcessor, long pubEndpointId) {
            return new Connection<>(recProcessor, pubProcessor, recEndpointId, pubEndpointId, connectionTransforms);
        }
    }
}
