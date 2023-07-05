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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.reactive.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;


public abstract class AbstractController<
        PROCESSOR_ID, INPUT, OUTPUT,
        REQ extends AbstractRequest<?, ?, INPUT>,
        PROCESSOR extends AbstractProcessor<INPUT, OUTPUT, ?, PROCESSOR>,
        CONTROLLER extends AbstractController<PROCESSOR_ID, INPUT, OUTPUT, ?, PROCESSOR, CONTROLLER>
        > extends Actor<CONTROLLER> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractController.class);
    private final Context context;

    private final Map<PROCESSOR_ID, Actor.Driver<PROCESSOR>> processors;

    AbstractController(Driver<CONTROLLER> driver, Context context, Supplier<String> debugName) {
        super(driver, debugName);
        this.context = context;
        this.processors = new HashMap<>();
    }

    public void initialise() {
        setUpUpstreamControllers();
    }

    protected abstract void setUpUpstreamControllers();

    ControllerRegistry registry() {
        return context.registry();
    }

    protected Driver<Monitor> monitor() {
        return context.monitor();
    }

    ReasonerPlanner planner() {
        return context.planner();
    }

    AbstractProcessor.Context processorContext() {
        return context.processor();
    }

    /*
     * Called on the controller that owns the requesting processor
     */
    public abstract void routeConnectionRequest(REQ connectionRequest);

    /*
     * Called on the target controller
     */
    <RECEIVED_REQ extends AbstractRequest<?, PROCESSOR_ID, OUTPUT>> void establishProcessorConnection(RECEIVED_REQ req) {
        getOrCreateProcessor(req.bounds()).execute(actor -> actor.establishConnection(req));
    }

    Driver<PROCESSOR> getOrCreateProcessor(PROCESSOR_ID processorId) {
        // TODO: We can do subsumption in the subtypes here
        return processors.computeIfAbsent(processorId, this::createProcessor);
    }

    private Actor.Driver<PROCESSOR> createProcessor(PROCESSOR_ID processorId) {
        Driver<PROCESSOR> processor = Actor.driver(
                d -> createProcessorFromDriver(d, processorId), context.executorService()
        );
        processor.execute(AbstractProcessor::setUp);
        return processor;
    }

    protected abstract PROCESSOR createProcessorFromDriver(Driver<PROCESSOR> processorDriver, PROCESSOR_ID processorId);

    @Override
    public void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Controller interrupted by resource close: {}", e.getMessage());
                context.registry().terminate(e);
                return;
            } else {
                LOG.debug("Controller interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        context.registry().terminate(e);
    }

    @Override
    public void terminate(Throwable cause) {
        super.terminate(cause);
        LOG.debug("Controller terminated.", cause);
        processors.values().forEach(p -> p.executeNext(a -> a.terminate(cause)));
    }

    public static class Context {

        private final AbstractProcessor.Context processorContext;
        private ActorExecutorGroup executorService;
        private final ControllerRegistry registry;
        private final Driver<Monitor> monitor;
        private final ReasonerPlanner planner;
        private final Tracer tracer;

        Context(ActorExecutorGroup executorService, ControllerRegistry registry, Driver<Monitor> monitor,
                ReasonerPlanner planner, ReasonerPerfCounters perfCounters, @Nullable Tracer tracer, boolean explainEnabled) {
            this.executorService = executorService;
            this.registry = registry;
            this.monitor = monitor;
            this.planner = planner;
            this.tracer = tracer;
            this.processorContext = new AbstractProcessor.Context(monitor, tracer, perfCounters, explainEnabled);
        }

        ActorExecutorGroup executorService() {
            return executorService;
        }

        void setExecutorService(ActorExecutorGroup executorService) {
            this.executorService = executorService;
        }

        private ControllerRegistry registry() {
            return registry;
        }

        private Driver<Monitor> monitor() {
            return monitor;
        }

        public ReasonerPlanner planner() {
            return planner;
        }

        Optional<Tracer> tracer() {
            return Optional.ofNullable(tracer);
        }

        public AbstractProcessor.Context processor() {
            return processorContext;
        }
    }
}
