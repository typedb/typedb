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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock.Connector;
import com.vaticle.typedb.core.reasoner.reactive.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;


public abstract class AbstractController<
        REACTIVE_BLOCK_ID, INPUT, OUTPUT,
        REQ extends Connector.AbstractRequest<?, ?, INPUT>,
        REACTIVE_BLOCK extends AbstractReactiveBlock<INPUT, OUTPUT, ?, REACTIVE_BLOCK>,
        CONTROLLER extends AbstractController<REACTIVE_BLOCK_ID, INPUT, OUTPUT, ?, REACTIVE_BLOCK, CONTROLLER>
        > extends Actor<CONTROLLER> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractController.class);
    private final Context context;

    private boolean terminated;
    protected final Map<REACTIVE_BLOCK_ID, Actor.Driver<REACTIVE_BLOCK>> reactiveBlocks;

    protected AbstractController(Driver<CONTROLLER> driver, Context context, Supplier<String> debugName) {
        super(driver, debugName);
        this.context = context;
        this.reactiveBlocks = new HashMap<>();
        this.terminated = false;
    }

    public void initialise() {
        setUpUpstreamControllers();
    }

    protected abstract void setUpUpstreamControllers();

    protected Registry registry() {
        return context.registry();
    }

    protected Driver<Monitor> monitor() {
        return context.monitor();
    }

    protected AbstractReactiveBlock.Context reactiveBlockContext() {
        return context.reactiveBlock();
    }

    public abstract void routeConnectionRequest(REQ connectionRequest);

    public void establishReactiveBlockConnection(Connector<REACTIVE_BLOCK_ID, OUTPUT> connector) {
        if (isTerminated()) return;
        getOrCreateReactiveBlock(connector.bounds()).execute(actor -> actor.establishConnection(connector));
    }

    public Driver<REACTIVE_BLOCK> getOrCreateReactiveBlock(REACTIVE_BLOCK_ID reactiveBlockId) {
        // TODO: We can do subsumption in the subtypes here
        return reactiveBlocks.computeIfAbsent(reactiveBlockId, this::createReactiveBlock);
    }

    private Actor.Driver<REACTIVE_BLOCK> createReactiveBlock(REACTIVE_BLOCK_ID reactiveBlockId) {
        Driver<REACTIVE_BLOCK> reactiveBlock = Actor.driver(
                d -> createReactiveBlockFromDriver(d, reactiveBlockId), context.executorService()
        );
        reactiveBlock.execute(AbstractReactiveBlock::setUp);
        return reactiveBlock;
    }

    protected abstract REACTIVE_BLOCK createReactiveBlockFromDriver(Driver<REACTIVE_BLOCK> reactiveBlockDriver, REACTIVE_BLOCK_ID reactiveBlockId);

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

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        terminated = true;
        reactiveBlocks.values().forEach(p -> p.execute(actor -> actor.terminate(cause)));
    }

    public boolean isTerminated() {
        return terminated;
    }

    public static class Context {

        private final AbstractReactiveBlock.Context reactiveBlockContext;
        private ActorExecutorGroup executorService;
        private final Registry registry;
        private final Driver<Monitor> monitor;
        private final Tracer tracer;

        Context(ActorExecutorGroup executorService, Registry registry, Driver<Monitor> monitor,
                @Nullable Tracer tracer) {
            this.executorService = executorService;
            this.registry = registry;
            this.monitor = monitor;
            this.tracer = tracer;
            this.reactiveBlockContext = new AbstractReactiveBlock.Context(monitor, tracer);
        }

        public ActorExecutorGroup executorService() {
            return executorService;
        }

        public void setExecutorService(ActorExecutorGroup executorService) {
            this.executorService = executorService;
        }

        public Registry registry() {
            return registry;
        }

        public Driver<Monitor> monitor() {
            return monitor;
        }

        public Optional<Tracer> tracer() {
            return Optional.ofNullable(tracer);
        }

        public AbstractReactiveBlock.Context reactiveBlock() {
            return reactiveBlockContext;
        }

    }
}
