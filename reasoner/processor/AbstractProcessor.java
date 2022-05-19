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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.controller.AbstractController;
import com.vaticle.typedb.core.reasoner.processor.Connector.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Identifier;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Subscriber;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.ReactiveIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public abstract class AbstractProcessor<INPUT, OUTPUT,
        REQ extends AbstractRequest<?, ?, INPUT>,
        REACTIVE_BLOCK extends AbstractProcessor<INPUT, OUTPUT, REQ, REACTIVE_BLOCK>> extends Actor<REACTIVE_BLOCK> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractProcessor.class);

    private final Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, REACTIVE_BLOCK, ?>> controller;
    private final Context context;
    private final Map<Identifier<?, ?>, Input<INPUT>> inputs;  // TODO: inputPorts (sweeping rename)
    private final Map<Identifier<?, ?>, Output<OUTPUT>> outputs;
    private final Map<Pair<Identifier<?, ?>, Identifier<?, ?>>, Runnable> pullRetries;
    private Reactive.Stream<OUTPUT,OUTPUT> initialReactive;
    private boolean terminated;
    private long reactiveCounter;

    protected AbstractProcessor(Driver<REACTIVE_BLOCK> driver,
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

    protected void setInitialReactive(Reactive.Stream<OUTPUT, OUTPUT> initialReactive) {
        this.initialReactive = initialReactive;
    }

    public Reactive.Stream<OUTPUT,OUTPUT> outputRouter() {  // TODO: initialReactive or hubReactive
        return initialReactive;
    }

    public void rootPull() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    public void pull(Identifier<?, ?> outputId) {
        outputs.get(outputId).pull();
    }

    public void receive(Identifier<?, ?> inputId, INPUT packet, Identifier<?, INPUT> publisherId) {
        inputs.get(inputId).receive(publisherId, packet);
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
        controller.execute(actor -> actor.routeConnectionRequest(req));
    }

    public void establishConnection(Connector<?, OUTPUT> connector) {
        if (isTerminated()) return;
        Output<OUTPUT> output = createOutput();
        output.setSubscriber(connector.inputId());
        connector.connectViaTransforms(outputRouter(), output);
        connector.inputId().processor().execute(
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
    public void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Processor interrupted by resource close: {}", e.getMessage());
                controller.execute(actor -> actor.exception(e));
                return;
            } else {
                LOG.debug("Processor interrupted by TypeDB exception: {}", e.getMessage());
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

    public static class Context {

        private final Driver<Monitor> monitor;
        private final Tracer tracer;

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

}
