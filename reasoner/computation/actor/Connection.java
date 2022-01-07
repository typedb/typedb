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
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public class Connection<PACKET, PROCESSOR extends Processor<PACKET, ?, PROCESSOR>, PUB_PROCESSOR extends Processor<?, PACKET, PUB_PROCESSOR>> {

    private final Actor.Driver<PROCESSOR> recProcessor;
    private final Actor.Driver<PUB_PROCESSOR> provProcessor;
    private final long recEndpointId;
    private final long provEndpointId;
    private final List<Function<Receiver.Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms;

    /**
     * Connects a processor outlet (upstream, publishing) to another processor's inlet (downstream, subscribing)
     */
    Connection(Actor.Driver<PROCESSOR> recProcessor, Actor.Driver<PUB_PROCESSOR> provProcessor, long recEndpointId,
               long provEndpointId,
               List<Function<Receiver.Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms) {
        this.recProcessor = recProcessor;
        this.provProcessor = provProcessor;
        this.recEndpointId = recEndpointId;
        this.provEndpointId = provEndpointId;
        this.connectionTransforms = connectionTransforms;
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

    public Receiver.Subscriber<PACKET> applyConnectionTransforms(Processor.OutletEndpoint<PACKET> upstreamEndpoint) {
        assert upstreamEndpoint.id() == provEndpointId;
        Receiver.Subscriber<PACKET> op = upstreamEndpoint;
        for (Function<Receiver.Subscriber<PACKET>, Reactive<PACKET, PACKET>> t : connectionTransforms) {
            op = t.apply(op);
        }
        return op;
    }

    public long providingEndpointId() {
        return provEndpointId;
    }

    public static abstract class Request<
            PUB_CID, PUB_PROC_ID,
            PUB_C extends Controller<PUB_PROC_ID, PACKET, ?, PUB_C>,
            PACKET,
            PROCESSOR extends Processor<PACKET, ?, PROCESSOR>,
            REQ extends Request<PUB_CID, PUB_PROC_ID, PUB_C, PACKET, PROCESSOR, REQ>
            > {

        private final PUB_CID provControllerId;
        private final Actor.Driver<PROCESSOR> recProcessor;
        private final long recEndpointId;
        private final List<Function<Receiver.Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms;
        private PUB_PROC_ID provProcessorId;

        protected Request(Actor.Driver<PROCESSOR> recProcessor, long recEndpointId, PUB_CID provControllerId,
                          PUB_PROC_ID provProcessorId) {
            this.recProcessor = recProcessor;
            this.recEndpointId = recEndpointId;
            this.provControllerId = provControllerId;
            this.provProcessorId = provProcessorId;
            this.connectionTransforms = new ArrayList<>();
        }

        public Processor.ConnectionBuilder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, PUB_C> createConnectionBuilder(Actor.Driver<PUB_C> pubController) {
            return new Processor.ConnectionBuilder<>(pubController, this);
        }

        public abstract Processor.ConnectionBuilder<PUB_PROC_ID, PACKET, REQ, PROCESSOR, ?> getBuilder(ControllerRegistry registry);

        public void withMap(Function<PACKET, PACKET> function) {
            connectionTransforms.add(r -> {
                Reactive<PACKET, PACKET> op = noOp();
                op.map(function).publishTo(r);
                return op;
            });
        }

        public void withNewProcessorId(PUB_PROC_ID newPID) {
            provProcessorId = newPID;
        }

        public Actor.Driver<PROCESSOR> recProcessor() {
            return recProcessor;
        }

        public PUB_CID pubControllerId() {
            return provControllerId;
        }

        public PUB_PROC_ID pubProcessorId() {
            return provProcessorId;
        }

        public long recEndpointId() {
            return recEndpointId;
        }

        public List<Function<Receiver.Subscriber<PACKET>, Reactive<PACKET, PACKET>>> connectionTransforms() {
            return connectionTransforms;
        }
    }
}
