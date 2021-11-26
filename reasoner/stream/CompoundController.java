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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.stream.Processor.InletManager;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;

import java.util.HashMap;
import java.util.Map;

public class CompoundController extends Controller<ConceptMap, ConceptMap, ConceptMap> {

    private final ResolverRegistry registry;
    private final Map<Resolvable<?>, Actor.Driver<Controller<ConceptMap, ConceptMap, ConceptMap>>> conclusionControllers;
//    private final Map<Resolvable<?>, Actor.Driver<ConclusionController>> downstreamConrollers;  // TODO: Ought to work but generics don't play nicely

    protected CompoundController(Driver<Controller<ConceptMap, ConceptMap, ConceptMap>> driver, String name,
                                 ActorExecutorGroup executorService, ResolverRegistry registry) {
        super(driver, name, , executorService);
        this.registry = registry;
        this.conclusionControllers = new HashMap<>();
    }

    @Override
    protected Operation<ConceptMap, ConceptMap> operation(ConceptMap bounds) {
        return new CompoundOperation(conclusionControllers);
    }

    private static Operation<ConceptMap, ConceptMap> defineOperation(ConceptMap bounds) {
        // This is a static method so that we don't accidentally leak state from the controller into the operation
        // TODO: An interesting point here is the initialisation. When we start we don't have any answers and so we want to create the first downstream
        //  the controller should initialise by constructing the first downstream and supplying it on or just after construction.
        Operation<?, ConceptMap> op1 = Operation.input();
        Operation<ConceptMap, InletManager<ConceptMap>> op = op1.forEach(a -> {
            processor().requestNewInlet(a)
        });
        return op;
    }

    private InletManager<ConceptMap> createNextInlet(ConceptMap answer) {
        // TODO: Maybe this is overcomplicated and we should use the registry instead. We should really consider this point.
        Resolvable<?> nextResolvable = null;  // TODO: Somehow we'll get the next resolvable
        // Get the Controller
        Driver<Controller<ConceptMap, ConceptMap, ConceptMap>> downstreamController = conclusionControllers.computeIfAbsent(nextResolvable, a -> registry.registerController(nextResolvable));

        ConceptMap bounds = null;
//        downstreamController.execute(actor -> actor.newOutlet(bounds)); // TODO: We shouldn't create a new outlet, we should be creating a new inlet
    }

}
