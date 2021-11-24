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

package com.vaticle.typedb.core.reasoner.stream;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.pattern.Pattern;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;


public abstract class Controller<INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Outlet<OUTPUT>, INLET_CONTROLLER extends Controller.InletController<INPUT, INLET>, OUTLET_CONTROLLER extends Controller.OutletController<OUTPUT, OUTLET>> extends Actor<Controller<INPUT, OUTPUT, INLET, OUTLET, INLET_CONTROLLER, OUTLET_CONTROLLER>> {

    private final ActorExecutorGroup executorService;
    private final Map<IDENTIFIER, Actor.Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>>> processors;
    private final Map<UPSTREAM_IDENTIFIER, Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>>> processorRequesters;

    protected Controller(Driver<CONTROLLER> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name);
        this.executorService = executorService;
        this.processors = new HashMap<>();
        this.processorRequesters = new HashMap<>();
    }

    protected abstract Processor.InletManager<INPUT> createInletManager(IDENTIFIER id);

    protected abstract Processor.OutletManager<OUTPUT> createOutletManager(IDENTIFIER id);

    protected abstract Operation<INPUT, OUTPUT> operation(IDENTIFIER id);

    Actor.Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>> buildProcessor(IDENTIFIER id) {
        Actor.Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>> processor = Actor.driver(
                driver -> new Processor<>(driver, driver(), id.toString(), operation(id), createInletManager(id), createOutletManager(id)), executorService);
        processors.put(id, processor);
        return processor;
    }

    public void receiveDownstreamProcessorRequest(UPSTREAM_IDENTIFIER id, Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>> requester) {
        // Message downstream controller responsible for creating processors as per the id.
        Driver<? extends Controller<UPSTREAM_IDENTIFIER, ?, ?, INPUT, ?>> controller = getControllerForId(id);
        processorRequesters.put(id, requester);
        controller.execute(actor -> actor.receiveProcessorRequest(id, driver()));
    }

    protected abstract Driver<? extends Controller<UPSTREAM_IDENTIFIER, ?, ?, INPUT, ?>> getControllerForId(UPSTREAM_IDENTIFIER id);  // TODO: Looks up the downstream controller by (pattern, bounds), either via registry or has already stored them.

    public void receiveProcessorRequest(IDENTIFIER id, Driver<? extends Controller<?, IDENTIFIER, OUTPUT, ?, ?>> requester) {
        Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>> processor = processors.computeIfAbsent(id, this::buildProcessor);
        requester.execute(actor -> actor.receiveRequestedProcessor(id, processor));
    }

    public <UPSTREAM_INPUT> void receiveRequestedProcessor(UPSTREAM_IDENTIFIER id, Driver<Processor<UPSTREAM_IDENTIFIER, UPSTREAM_INPUT, INPUT>> processor) {
        Driver<Processor<UPSTREAM_IDENTIFIER, INPUT, OUTPUT>> requester = processorRequesters.remove(id);
        assert requester != null;
        requester.execute(actor -> actor.inletManager().add(processor));
    }

    public FunctionalIterator<ConceptMap> createTraversal(Pattern pattern, ConceptMap bounds) {  // TODO: This framework shouldn't know about Patterns or ConceptMaps
        return null; // TODO
    }

    static class Source<INPUT> {
        public static <INPUT> Source<INPUT> fromIteratorSupplier(Supplier<FunctionalIterator<ConceptMap>> traversal) {
            return null;  // TODO
        }

        public Operation<INPUT, INPUT> asOperation() {
            return null; // TODO
        }
    }

    @Override
    protected void exception(Throwable e) {

    }
}
