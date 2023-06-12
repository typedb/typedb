/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
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

public abstract class AbstractProcessor<
        INPUT, OUTPUT, REQ extends AbstractRequest<?, ?, INPUT>,
        PROCESSOR extends AbstractProcessor<INPUT, OUTPUT, REQ, PROCESSOR>> extends Actor<PROCESSOR> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractProcessor.class);

    private final Driver<? extends AbstractController<?, INPUT, OUTPUT, REQ, PROCESSOR, ?>> controller;
    private final Context context;
    private final Map<Identifier, InputPort<INPUT>> inputPorts;
    private final Map<Identifier, OutputPort<OUTPUT>> outputPorts;
    private final Map<Pair<Identifier, Identifier>, Runnable> pullRetries;
    private Stream<OUTPUT, OUTPUT> hubReactive;
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

    protected Stream<OUTPUT, OUTPUT> hubReactive() {
        return hubReactive;
    }

    public void rootPull() {
        throw TypeDBException.of(ILLEGAL_OPERATION);
    }

    void pull(Identifier outputPortId) {
        outputPorts.get(outputPortId).pull();
    }

    void receive(Identifier inputPortId, INPUT packet, Identifier publisherId) {
        inputPorts.get(inputPortId).receive(publisherId, packet);
    }

    public <PACKET> void schedulePullRetry(Publisher<PACKET> publisher, Subscriber<PACKET> subscriber) {
        pullRetries.put(new Pair<>(publisher.identifier(), subscriber.identifier()), () -> publisher.pull(subscriber));
        driver().execute(actor -> actor.pullRetry(publisher.identifier(), subscriber.identifier()));
    }

    void pullRetry(Identifier publisher, Identifier subscriber) {
        tracer().ifPresent(tracer -> tracer.pullRetry(subscriber, publisher));
        pullRetries.get(new Pair<>(publisher, subscriber)).run();
    }

    protected void requestConnection(REQ req) {
        controller.execute(actor -> actor.routeConnectionRequest(req));
    }

    public <RECEIVED_REQ extends AbstractRequest<?, ?, OUTPUT>> void establishConnection(RECEIVED_REQ request) {
        OutputPort<OUTPUT> outputPort = createOutputPort();
        outputPort.setInputPort(request.inputPortId(), request.requestingProcessor());
        request.connectViaTransforms(hubReactive(), outputPort);
        request.requestingProcessor().execute(
                actor -> actor.finishConnection(request.inputPortId(), driver(), outputPort.identifier())
        );
    }

    void finishConnection(
            Identifier inputPortId, Driver<? extends AbstractProcessor<?, INPUT, ?, ?>> outputPortProcessor,
            Identifier outputPortId
    ) {
        InputPort<INPUT> input = inputPorts.get(inputPortId);
        input.setOutputPort(outputPortId, outputPortProcessor);
        input.pull();
    }

    protected InputPort<INPUT> createInputPort() {
        InputPort<INPUT> inputPort = new InputPort<>(this);
        inputPorts.put(inputPort.identifier(), inputPort);
        return inputPort;
    }

    private OutputPort<OUTPUT> createOutputPort() {
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

    public void onFinished(Identifier finishable) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public void exception(Throwable e) {
        controller.executeNext(controller -> controller.exception(e));
    }

    private long incrementReactiveCounter() {
        reactiveCounter += 1;
        return reactiveCounter;
    }

    public Identifier registerReactive(Reactive reactive) {
        return new ReactiveIdentifier<>(driver(), reactive, incrementReactiveCounter());
    }

    public static class Context {

        private final Driver<Monitor> monitor;
        private final Tracer tracer;
        private final ReasonerPerfCounters perfCounters;
        private final boolean explainEnabled;

        public Context(Driver<Monitor> monitor, @Nullable Tracer tracer, ReasonerPerfCounters perfCounters, boolean explainEnabled) {
            this.monitor = monitor;
            this.tracer = tracer;
            this.perfCounters = perfCounters;
            this.explainEnabled = explainEnabled;
        }

        public Optional<Tracer> tracer() {
            return Optional.ofNullable(tracer);
        }

        public Driver<Monitor> monitor() {
            return monitor;
        }

        public ReasonerPerfCounters perfCounters() {
            return perfCounters;
        }

        public boolean explainEnabled() {
            return explainEnabled;
        }
    }

}
