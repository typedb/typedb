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

package com.vaticle.typedb.core.reasoner.processor;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.controller.AbstractController;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET,
        CONTROLLER extends AbstractController<BOUNDS, ?, PACKET, ?, ?, ?>> {

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

    void connectViaTransforms(Reactive.Stream<PACKET, PACKET> toConnect, OutputPort<PACKET> output) {
        Reactive.Publisher<PACKET> op = toConnect;
        for (Function<PACKET, PACKET> t : transforms) op = op.map(t);
        op.registerSubscriber(output);
    }

    public AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET, CONTROLLER> withMap(Function<PACKET, PACKET> function) {
        transforms.add(function);
        return this;
    }

    public AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET, CONTROLLER> withNewBounds(BOUNDS newBounds) {
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
