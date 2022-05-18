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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.controller.AbstractController;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Identifier;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Reactive.Subscriber;
import com.vaticle.typedb.core.reasoner.reactive.common.ReactiveIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class AbstractReactiveBlock<INPUT, OUTPUT,
        REQ extends AbstractReactiveBlock.Connector.AbstractRequest<?, ?, INPUT>,
        REACTIVE_BLOCK extends AbstractReactiveBlock<INPUT, OUTPUT, REQ, REACTIVE_BLOCK>> extends Actor<REACTIVE_BLOCK> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractReactiveBlock.class);

    private final Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, ?>> controller;
    private final Context context;
    private final Map<Identifier<?, ?>, Input<INPUT>> inputs;
    private final Map<Identifier<?, ?>, Output<OUTPUT>> outputs;
    private final Map<Pair<Identifier<?, ?>, Identifier<?, ?>>, Runnable> pullRetries;
    private Reactive.Stream<OUTPUT,OUTPUT> outputRouter;
    private boolean terminated;
    private long reactiveCounter;

    protected AbstractReactiveBlock(Driver<REACTIVE_BLOCK> driver,
                                    Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, ?>> controller,
                                    Context context, Supplier<String> debugName) {
        super(driver, debugName);
        this.controller = controller;
        this.context = context;
        this.inputs = new HashMap<>();
        this.outputs = new HashMap<>();
        this.reactiveCounter = 0;
        this.pullRetries = new HashMap<>();
    }

    public abstract void setUp();

    protected void setOutputRouter(Reactive.Stream<OUTPUT, OUTPUT> outputRouter) {
        this.outputRouter = outputRouter;
    }

    public Reactive.Stream<OUTPUT,OUTPUT> outputRouter() {
        return outputRouter;
    }

    public void rootPull() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    public void pull(Identifier<?, ?> outputId) {
        outputs.get(outputId).pull();
    }

    public void receive(Identifier<?, INPUT> publisherId, INPUT input, Identifier<?, ?> inputId) {
        inputs.get(inputId).receive(publisherId, input);
    }

    public <PACKET> void schedulePullRetry(Publisher<PACKET> publisher, Subscriber<PACKET> subscriber) {
        pullRetries.put(new Pair<>(publisher.identifier(), subscriber.identifier()), () -> publisher.pull(subscriber));
        driver().execute(actor -> actor.pullRetry(publisher.identifier(), subscriber.identifier()));
    }

    protected void pullRetry(Identifier<?, ?> publisher, Identifier<?, ?> subscriber) {
        tracer().ifPresent(tracer -> tracer.pullRetry(subscriber, publisher));
        pullRetries.get(new Pair<Identifier<?, ?>, Identifier<?, ?>>(publisher, subscriber)).run();
    }

    protected void requestConnection(REQ req) {
        if (isTerminated()) return;
        controller.execute(actor -> actor.resolveController(req));
    }

    public void establishConnection(Connector<?, OUTPUT> connector) {
        if (isTerminated()) return;
        Output<OUTPUT> output = createOutput();
        output.setSubscriber(connector.inputId());
        connector.connectViaTransforms(outputRouter(), output);
        connector.inputId().reactiveBlock().execute(
                actor -> actor.finishConnection(connector.inputId(), output.identifier()));
    }

    protected void finishConnection(Identifier<INPUT, ?> inputId, Identifier<?, INPUT> outputId) {
        Input<INPUT> input = inputs.get(inputId);
        input.setOutput(outputId);
        input.pull();
    }

    protected Input<INPUT> createInput() {
        Input<INPUT> input = new Input<>(this);
        inputs.put(input.identifier(), input);
        return input;
    }

    protected Output<OUTPUT> createOutput() {
        Output<OUTPUT> output = new Output<>(this);
        outputs.put(output.identifier(), output);
        return output;
    }

    public Driver<Monitor> monitor() {
        return context.monitor();
    }

    Optional<Tracer> tracer() {
        return context.tracer();
    }

    public Context context() {
        return context;
    }

    public void onFinished(Identifier<?, ?> finishable) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("ReactiveBlock interrupted by resource close: {}", e.getMessage());
                controller.execute(actor -> actor.exception(e));
                return;
            } else {
                LOG.debug("ReactiveBlock interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        controller.execute(actor -> actor.exception(e));
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public long incrementReactiveCounter() {
        reactiveCounter += 1;
        return reactiveCounter;
    }

    public Identifier<INPUT, OUTPUT> registerReactive(Reactive reactive) {
        return new ReactiveIdentifier<>(driver(), reactive, incrementReactiveCounter());
    }

    public static class Connector<BOUNDS, PACKET> {

        private final Identifier<PACKET, ?> inputId;
        private final List<Function<PACKET, PACKET>> transforms;
        private final BOUNDS bounds;

        public Connector(Identifier<PACKET, ?> inputId, BOUNDS bounds) {
            this.inputId = inputId;
            this.transforms = new ArrayList<>();
            this.bounds = bounds;
        }

        public Connector(Identifier<PACKET, ?> inputId, BOUNDS bounds, List<Function<PACKET, PACKET>> transforms) {
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

        public Identifier<PACKET, ?> inputId() {
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

        public abstract static class AbstractRequest<CONTROLLER_ID, BOUNDS, PACKET> {

            private final CONTROLLER_ID controllerId;
            private final BOUNDS bounds;
            private final Identifier<PACKET, ?> inputId;

            protected AbstractRequest(Identifier<PACKET, ?> inputId, CONTROLLER_ID controllerId,
                                      BOUNDS bounds) {
                this.inputId = inputId;
                this.controllerId = controllerId;
                this.bounds = bounds;
            }

            public Identifier<PACKET, ?> inputId() {
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
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                AbstractRequest<?, ?, ?> request = (AbstractRequest<?, ?, ?>) o;
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

    public static class Context {

        private final Driver<Monitor> monitor;
        @Nullable private final Tracer tracer;

        public Context(Driver<Monitor> monitor, @Nullable Tracer tracer) {
            this.monitor = monitor;
            this.tracer = tracer;
        }

        public Optional<Tracer> tracer() {
            return Optional.ofNullable(tracer);
        }

        public Driver<Monitor> monitor() {
            return monitor;
        }

    }

    /**
     * Governs an input to a reactiveBlock
     */
    public static class Input<PACKET> implements Publisher<PACKET> {

        private final Identifier<PACKET, ?> identifier;
        private final AbstractReactiveBlock<PACKET, ?, ?, ?> reactiveBlock;
        private final AbstractReactive.PublisherDelegateImpl<PACKET> publisherActions;
        private boolean ready;
        private Identifier<?, PACKET> providingOutput;
        private Subscriber<PACKET> subscriber;

        public Input(AbstractReactiveBlock<PACKET, ?, ?, ?> reactiveBlock) {
            this.reactiveBlock = reactiveBlock;
            this.identifier = reactiveBlock.registerReactive(this);
            this.ready = false;
            this.publisherActions = new AbstractReactive.PublisherDelegateImpl<>(this, reactiveBlock.context());
        }

        @Override
        public AbstractReactiveBlock<?, ?, ?, ?> reactiveBlock() {
            return reactiveBlock;
        }

        @Override
        public Identifier<PACKET, ?> identifier() {
            return identifier;
        }

        public void setOutput(Identifier<?, PACKET> outputId) {
            assert providingOutput == null;
            providingOutput = outputId;
            reactiveBlock().monitor().execute(actor -> actor.registerPath(identifier(), outputId));
            assert !ready;
            ready = true;
        }

        public void pull() {
            pull(subscriber);
        }

        @Override
        public void pull(Subscriber<PACKET> subscriber) {
            assert subscriber.equals(this.subscriber);
            reactiveBlock().tracer().ifPresent(tracer -> tracer.pull(subscriber.identifier(), identifier()));
            if (ready) providingOutput.reactiveBlock().execute(actor -> actor.pull(providingOutput));
        }

        @Override
        public void registerSubscriber(Subscriber<PACKET> subscriber) {
            assert this.subscriber == null;
            this.subscriber = subscriber;
            subscriber.registerPublisher(this);
        }

        @Override
        public <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function) {
            return publisherActions.map(this, function);
        }

        @Override
        public <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function) {
            return publisherActions.flatMap(this, function);
        }

        @Override
        public Stream<PACKET, PACKET> buffer() {
            return publisherActions.buffer(this);
        }

        @Override
        public Stream<PACKET, PACKET> distinct() {
            return publisherActions.distinct(this);
        }

        public void receive(Identifier<?, PACKET> outputId, PACKET packet) {
            reactiveBlock().tracer().ifPresent(tracer -> tracer.receive(outputId, identifier(), packet));
            subscriber.receive(this, packet);
        }

        @Override
        public String toString() {
            return reactiveBlock.debugName().get() + ":" + getClass().getSimpleName();
        }

    }

    /**
     * Governs an output from a reactiveBlock
     */
    public static class Output<PACKET> implements Subscriber<PACKET> {

        private final Identifier<?, PACKET> identifier;
        private final AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock;
        private final AbstractReactive.SubscriberDelegateImpl<PACKET> subscriberActions;
        private Identifier<PACKET, ?> receivingInput;
        private Publisher<PACKET> publisher;

        public Output(AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock) {
            this.reactiveBlock = reactiveBlock;
            this.identifier = reactiveBlock().registerReactive(this);
            this.subscriberActions = new AbstractReactive.SubscriberDelegateImpl<>(this, reactiveBlock().context());
        }

        @Override
        public Identifier<?, PACKET> identifier() {
            return identifier;
        }

        @Override
        public AbstractReactiveBlock<?, PACKET, ?, ?> reactiveBlock() {
            return reactiveBlock;
        }

        @Override
        public void receive(Publisher<PACKET> publisher, PACKET packet) {
            subscriberActions.traceReceive(publisher, packet);
            receivingInput.reactiveBlock().execute(actor -> actor.receive(identifier(), packet, receivingInput));
        }

        public void pull() {
            assert publisher != null;
            reactiveBlock().context().tracer().ifPresent(tracer -> tracer.pull(receivingInput, identifier()));
            publisher.pull(this);
        }

        @Override
        public void registerPublisher(Publisher<PACKET> publisher) {
            assert this.publisher == null;
            this.publisher = publisher;
            subscriberActions.registerPath(publisher);
        }

        public void setSubscriber(Identifier<PACKET, ?> inputId) {
            assert receivingInput == null;
            receivingInput = inputId;
        }

        @Override
        public String toString() {
            return reactiveBlock().debugName().get() + ":" + getClass().getSimpleName();
        }
    }

}
