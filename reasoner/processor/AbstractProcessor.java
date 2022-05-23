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
import com.vaticle.typedb.core.reasoner.processor.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Identifier;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
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

public abstract class AbstractProcessor<
        INPUT, OUTPUT, REQ extends AbstractRequest<?, ?, INPUT, ?>,
        PROCESSOR extends AbstractProcessor<INPUT, OUTPUT, REQ, PROCESSOR>> extends Actor<PROCESSOR> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractProcessor.class);

    private final Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller;
    private final Context context;
    private final Map<Identifier<?, ?>, InputPort<INPUT>> inputPorts;  // TODO: inputPorts (sweeping rename)
    private final Map<Identifier<?, ?>, OutputPort<OUTPUT>> outputPorts;
    private final Map<Pair<Identifier<?, ?>, Identifier<?, ?>>, Runnable> pullRetries;
    private Stream<OUTPUT,OUTPUT> hubReactive;
    private boolean terminated;
    private long reactiveCounter;

    protected AbstractProcessor(Driver<PROCESSOR> driver,
                                Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller,
                                Context context, Supplier<String> debugName) {
        super(driver, debugName);
        this.controller = controller;
        this.context = context;
        this.inputPorts = new HashMap<>();
        this.outputPorts = new HashMap<>();
        this.reactiveCounter = 0;
        this.pullRetries = new HashMap<>();
    }

    public abstract void setUp();

    protected void setHubReactive(Stream<OUTPUT, OUTPUT> hubReactive) {
        this.hubReactive = hubReactive;
    }

    public Stream<OUTPUT,OUTPUT> outputRouter() {
        return hubReactive;
    }

    public void rootPull() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    public void pull(Identifier<?, ?> outputPortId) {
        outputPorts.get(outputPortId).pull();
    }

    public void receive(Identifier<?, ?> inputPortId, INPUT packet, Identifier<?, INPUT> publisherId) {
        inputPorts.get(inputPortId).receive(publisherId, packet);
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

    public <RECEIVED_REQ extends AbstractRequest<?, ?, OUTPUT, ?>> void establishConnection(RECEIVED_REQ request) {
        if (isTerminated()) return;
        OutputPort<OUTPUT> outputPort = createOutputPort();
        outputPort.setInputPort(request.inputPortId());
        request.connectViaTransforms(outputRouter(), outputPort);
        request.inputPortId().processor().execute(
                actor -> actor.finishConnection(request.inputPortId(), outputPort.identifier()));
    }

    protected void finishConnection(Identifier<INPUT, ?> inputPortId, Identifier<?, INPUT> outputPortId) {
        InputPort<INPUT> input = inputPorts.get(inputPortId);
        input.setOutputPort(outputPortId);
        input.pull();
    }

    protected InputPort<INPUT> createInputPort() {
        InputPort<INPUT> inputPort = new InputPort<>(this);
        inputPorts.put(inputPort.identifier(), inputPort);
        return inputPort;
    }

    protected OutputPort<OUTPUT> createOutputPort() {
        OutputPort<OUTPUT> outputPort = new OutputPort<>(this);
        outputPorts.put(outputPort.identifier(), outputPort);
        return outputPort;
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
