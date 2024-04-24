/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET> {

    private final Reactive.Identifier inputPortId;
    private final Actor.Driver<? extends AbstractProcessor<PACKET, ?, ?, ?>> inputPortProcessor;
    private final CONTROLLER_ID controllerId;
    private final Identifier id;
    private final List<Function<PACKET, PACKET>> transforms;
    private BOUNDS bounds;

    protected AbstractRequest(
            Reactive.Identifier inputPortId,
            Actor.Driver<?extends AbstractProcessor<PACKET, ?, ?, ?>> inputPortProcessor, CONTROLLER_ID controllerId,
            BOUNDS bounds
    ) {
        this.inputPortId = inputPortId;
        this.inputPortProcessor = inputPortProcessor;
        this.controllerId = controllerId;
        this.transforms = new ArrayList<>();
        this.bounds = bounds;
        this.id = new Identifier(inputPortId, controllerId, bounds);
    }

    public Identifier id() {
        return id;
    }

    Reactive.Identifier inputPortId() {
        return inputPortId;
    }

    Actor.Driver<? extends AbstractProcessor<PACKET, ?, ?, ?>> requestingProcessor() {
        return inputPortProcessor;
    }

    public CONTROLLER_ID controllerId() {
        return controllerId;
    }

    public BOUNDS bounds() {
        return bounds;
    }

    void connectViaTransforms(Stream<PACKET, PACKET> toConnect, OutputPort<PACKET> output) {
        Reactive.Publisher<PACKET> op = toConnect;
        for (Function<PACKET, PACKET> t : transforms) op = op.map(t);
        op.registerSubscriber(output);
    }

    public AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET> withMap(Function<PACKET, PACKET> function) {
        transforms.add(function);
        return this;
    }

    public AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET> withNewBounds(BOUNDS newBounds) {
        bounds = newBounds;
        return this;
    }

    public class Identifier {
        private final Reactive.Identifier inputPortId;
        private final CONTROLLER_ID controllerId;
        private final BOUNDS bounds;

        private Identifier(Reactive.Identifier inputPortId, CONTROLLER_ID controllerId, BOUNDS bounds) {
            this.inputPortId = inputPortId;
            this.controllerId = controllerId;
            this.bounds = bounds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Identifier that = (Identifier) o;
            // TODO: be wary with equality when conjunctions could be involved
            return inputPortId.equals(that.inputPortId) &&
                    controllerId.equals(that.controllerId) &&
                    bounds.equals(that.bounds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inputPortId, controllerId, bounds);
        }
    }
}
