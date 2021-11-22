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


public abstract class Controller<INPUT, OUTPUT, INLET extends Inlet<INPUT>, OUTLET extends Outlet<OUTPUT>, INLET_CONTROLLER extends Controller.InletController<INPUT, INLET>, OUTLET_CONTROLLER extends Controller.OutletController<OUTPUT, OUTLET>> extends Actor<Controller<INPUT, OUTPUT, INLET, OUTLET, INLET_CONTROLLER, OUTLET_CONTROLLER>> {

    private final ActorExecutorGroup executorService;
    private final boolean dynamicInlets;
    private final boolean dynamicOutlets;
    private final Map<IDENTIFIER, Actor.Driver<Processor<INPUT, OUTPUT>>> processors;

    protected Controller(Driver<Controller<INPUT, OUTPUT, INLET, OUTLET, INLET_CONTROLLER, OUTLET_CONTROLLER>> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name);
        this.executorService = executorService;
        this.dynamicInlets = dynamicInlets;
        this.dynamicOutlets = dynamicOutlets;
        this.processors = new HashMap<>();
    }

    protected abstract Operation<INPUT, OUTPUT> operation(IDENTIFIER id);

    Actor.Driver<Processor<INPUT, OUTPUT>> buildProcessor(IDENTIFIER id) {
        Actor.Driver<Processor<INPUT, OUTPUT>> processor = Actor.driver(
                driver -> new Processor<>(driver, "name", operation(id), dynamicInlets, dynamicOutlets), executorService);
        processors.put(id, processor);
        return processor;
    }

    public Driver<Processor<INPUT, OUTPUT>> attachProcessor(IDENTIFIER identifier) {
        // TODO: misleading naming, doesn't attach anything
        return processors.computeIfAbsent(identifier, i -> buildProcessor(identifier));
    }

    public <UPSTREAM_OUTPUT> void addInlet(Actor.Driver<Processor<INPUT, OUTPUT>> processor, Actor.Driver<Processor<UPSTREAM_OUTPUT, INPUT>> newInlet) {
        processor.execute(actor -> actor.inlet().add(newInlet));
    }

    protected abstract Operation<INPUT,OUTPUT> operation();

    protected abstract INLET_CONTROLLER inletController();

    protected abstract OUTLET_CONTROLLER outletController();

    public FunctionalIterator<ConceptMap> createTraversal(Pattern pattern, ConceptMap bounds) {
        return null; // TODO
    }

    static class Source<INPUT> {
        public static <INPUT> Source<INPUT> fromIterator(FunctionalIterator<INPUT> traversal) {
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
