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

import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.Output;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class Connector<BOUNDS, PACKET> {

    private final Reactive.Identifier<PACKET, ?> inputId;
    private final List<Function<PACKET, PACKET>> transforms;
    private final BOUNDS bounds;

    public Connector(Reactive.Identifier<PACKET, ?> inputId, BOUNDS bounds) {
        this.inputId = inputId;
        this.transforms = new ArrayList<>();
        this.bounds = bounds;
    }

    public Connector(Reactive.Identifier<PACKET, ?> inputId, BOUNDS bounds, List<Function<PACKET, PACKET>> transforms) {
        this.inputId = inputId;
        this.transforms = transforms;
        this.bounds = bounds;
    }

    public void connectViaTransforms(Reactive.Stream<PACKET, PACKET> toConnect, Output<PACKET> output) {
        Publisher<PACKET> op = toConnect;
        for (Function<PACKET, PACKET> t : transforms) op = op.map(t);
        op.registerSubscriber(output);
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
        return new Connector<>(inputId, bounds, newTransforms);
    }

    public Connector<BOUNDS, PACKET> withNewBounds(BOUNDS newBounds) {
        return new Connector<>(inputId, newBounds, transforms);
    }

    public abstract static class Request<CONTROLLER_ID, BOUNDS, PACKET> {

        private final CONTROLLER_ID controllerId;
        private final BOUNDS bounds;
        private final Reactive.Identifier<PACKET, ?> inputId;

        protected Request(Reactive.Identifier<PACKET, ?> inputId, CONTROLLER_ID controllerId,
                          BOUNDS bounds) {
            this.inputId = inputId;
            this.controllerId = controllerId;
            this.bounds = bounds;
        }

        public Reactive.Identifier<PACKET, ?> inputId() {
            return inputId;
        }

        public CONTROLLER_ID controllerId() {
            return controllerId;
        }

        public BOUNDS bounds() {
            return bounds;
        }

        @Override
        public boolean equals(Object o) {
            // TODO: be wary with request equality when conjunctions are involved
            // TODO: I think there's a subtle bug here where a conjunction could break down into two parts that according
            //  to conjunction comparison are the same, and therefore will not create separate processors for them
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request<?, ?, ?> request = (Request<?, ?, ?>) o;
            return inputId == request.inputId &&
                    controllerId.equals(request.controllerId) &&
                    bounds.equals(request.bounds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(controllerId, inputId, bounds);
        }

    }
}
