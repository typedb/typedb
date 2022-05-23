package com.vaticle.typedb.core.reasoner.processor;

import com.vaticle.typedb.core.reasoner.controller.AbstractController;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET,
        CONTROLLER extends AbstractController<BOUNDS, ?, PACKET, ?, ?, ?>> {

    // TODO: Should hold on to the processor that sent the request
    private final Reactive.Identifier<PACKET, ?> inputPortId;
    private final CONTROLLER_ID controllerId;
    private final List<Function<PACKET, PACKET>> transforms;
    private final Identifier id;
    private BOUNDS bounds;

    protected AbstractRequest(Reactive.Identifier<PACKET, ?> inputPortId, CONTROLLER_ID controllerId,
                              BOUNDS bounds) {
        this.inputPortId = inputPortId;
        this.controllerId = controllerId;
        this.transforms = new ArrayList<>();
        this.bounds = bounds;
        this.id = new Identifier(inputPortId, controllerId, bounds);
    }

    public Identifier id() {
        return id;
    }

    public Reactive.Identifier<PACKET, ?> inputPortId() {
        return inputPortId;
    }

    public CONTROLLER_ID controllerId() {
        return controllerId;
    }

    public BOUNDS bounds() {
        return bounds;
    }

    public void connectViaTransforms(Reactive.Stream<PACKET, PACKET> toConnect, OutputPort<PACKET> output) {
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
        private final Reactive.Identifier<PACKET, ?> inputPortId;
        private final CONTROLLER_ID controllerId;
        private final BOUNDS bounds;

        Identifier(Reactive.Identifier<PACKET, ?> inputPortId, CONTROLLER_ID controllerId, BOUNDS bounds) {
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
