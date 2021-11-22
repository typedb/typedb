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
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConcludableResolver;
import com.vaticle.typedb.core.reasoner.stream.Processor.Inlet;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;
import com.vaticle.typedb.core.reasoner.stream.Processor.Outlet;

import java.util.HashMap;
import java.util.Map;

public class CompoundController extends Controller<ConceptMap, ConceptMap, ConceptMap> {

    private final ResolverRegistry registry;
    private final Map<Resolvable<?>, Actor.Driver<Controller<ConceptMap, ConceptMap, ConceptMap>> downstreamResolvers;

    protected CompoundController(Driver<Controller<ConceptMap, ConceptMap, ConceptMap>> driver, String name,
                                 ActorExecutorGroup executorService, boolean dynamicInlets, boolean dynamicOutlets,
                                 ResolverRegistry registry) {
        super(driver, name, executorService, dynamicInlets, dynamicOutlets);
        this.registry = registry;
        this.downstreamResolvers = new HashMap<>();
    }

    @Override
    protected Operation<ConceptMap, ConceptMap> operation(ConceptMap bounds) {
        // TODO: Change source and sink to refer to inlets and outlets (except the traversalSource which is actually a
        //  source). These can then be typed and we can keep a record of whether we can message children to add extra inlets or outlets.

        // TODO: An interesting point here is the initialisation. When we start we don't have any answers and so we want to create the first downstream
        Operation<ConceptMap, ConceptMap> op1 = Operation.input();
        Operation<ConceptMap, Inlet<ConceptMap>> op = op1.flatMapOrRetry(a -> createNextDownstream(a));
        return op;
    }

    private Inlet<ConceptMap> createNextDownstream(ConceptMap answer) {
        // TODO: Maybe this is overcomplicated and we should use the registry instead. We should really consider this point.
        Resolvable<?> nextResolvable = null;  // TODO: Somehow we'll get the next resolvable
        // Get the Controller
        Driver<Controller<ConceptMap, ConceptMap, ConceptMap>> downstreamController = downstreamResolvers.computeIfAbsent(nextResolvable, a -> registry.registerController(nextResolvable));

        ConceptMap bounds = null;
        downstreamController.execute(actor -> actor.attachProcessor(bounds)); // TODO: Ideally block until receiving a response for this.

    }

}
