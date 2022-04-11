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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.reasoner.controller.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;


public abstract class Controller<
        REACTIVE_BLOCK_ID, INPUT, OUTPUT,
        REQ extends Connector.Request<?, ?, INPUT>,
        REACTIVE_BLOCK extends ReactiveBlock<INPUT, OUTPUT, ?, REACTIVE_BLOCK>,
        CONTROLLER extends Controller<REACTIVE_BLOCK_ID, INPUT, OUTPUT, ?, REACTIVE_BLOCK, CONTROLLER>
        > extends Actor<CONTROLLER> {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    private boolean terminated;
    private final ActorExecutorGroup executorService;
    private final Registry registry;
    protected final Map<REACTIVE_BLOCK_ID, Actor.Driver<REACTIVE_BLOCK>> reactiveBlocks;

    protected Controller(Driver<CONTROLLER> driver, ActorExecutorGroup executorService, Registry registry,
                         Supplier<String> debugName) {
        super(driver, debugName);
        this.executorService = executorService;
        this.reactiveBlocks = new HashMap<>();
        this.terminated = false;
        this.registry = registry;
    }

    public void initialise() {
        setUpUpstreamControllers();
    }

    protected abstract void setUpUpstreamControllers();

    protected Registry registry() {
        return registry;
    }

    protected abstract void resolveController(REQ connectionRequest);

    public void resolveReactiveBlock(Connector<REACTIVE_BLOCK_ID, OUTPUT> connector) {
        if (isTerminated()) return;
        createReactiveBlockIfAbsent(connector.bounds()).execute(actor -> actor.establishConnection(connector));
    }

    public Driver<REACTIVE_BLOCK> createReactiveBlockIfAbsent(REACTIVE_BLOCK_ID reactiveBlockId) {
        // TODO: We can do subsumption in the subtypes here
        return reactiveBlocks.computeIfAbsent(reactiveBlockId, this::createReactiveBlock);
    }

    private Actor.Driver<REACTIVE_BLOCK> createReactiveBlock(REACTIVE_BLOCK_ID reactiveBlockId) {
        if (isTerminated()) return null;  // TODO: Avoid returning null
        Driver<REACTIVE_BLOCK> reactiveBlock = Actor.driver(d -> createReactiveBlockFromDriver(d, reactiveBlockId), executorService);
        reactiveBlock.execute(ReactiveBlock::setUp);
        return reactiveBlock;
    }

    protected abstract REACTIVE_BLOCK createReactiveBlockFromDriver(Driver<REACTIVE_BLOCK> reactiveBlockDriver, REACTIVE_BLOCK_ID reactiveBlockId);

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Controller interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            } else {
                LOG.debug("Controller interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
        reactiveBlocks.values().forEach(p -> p.execute(actor -> actor.terminate(cause)));
    }

    public boolean isTerminated() {
        return terminated;
    }

}
