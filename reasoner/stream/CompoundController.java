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
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConcludableResolver;
import com.vaticle.typedb.core.reasoner.stream.Processor.Inlet;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;
import com.vaticle.typedb.core.reasoner.stream.Processor.Outlet;

import java.util.HashMap;
import java.util.Map;

public class CompoundController extends Controller {

    private final Map<ConceptMap, ProcessorRef<ConceptMap, ConceptMap, ?, ?, InletController.DynamicMulti<ConceptMap>, OutletController.Single<ConceptMap>>> compoundProcessors;
    private final ResolverRegistry registry;
    private final Map<Resolvable<?>, Controller<?, ?, ?, ?>> downstreamResolvers;

    protected CompoundController(Driver<Controller> driver, String name, ActorExecutorGroup executorService, ResolverRegistry registry) {
        super(driver, name, executorService);
        this.registry = registry;
        this.compoundProcessors = new HashMap<>();
        this.downstreamResolvers = new HashMap<Resolvable<?>, Processor<?, ?, ?, ?>>();
    }

    private void createCompoundProcessor(ConceptMap bounds) {
        // TODO: Change source and sink to refer to inlets and outlets (except the traversalSource which is actually a
        //  source). These can then be typed and we can keep a record of whether we can message children to add extra inlets or outlets.


        // TODO: An interesting point here is the initialisation. When we start we don't have any answers and so we want to create the first downstream
        Operation<ConceptMap, ConceptMap> op1 = Operation.input();
        Operation<ConceptMap, Inlet<ConceptMap>> op = op1.flatMapOrRetry(a -> createNextDownstream(a));

        OutletController.Single<ConceptMap> outletController = OutletController.single();
        InletController.DynamicMulti<ConceptMap> multiInletController = InletController.dynamicMulti();


        ProcessorRef<ConceptMap, ConceptMap, ?, ?, InletController.DynamicMulti<ConceptMap>, OutletController.Single<ConceptMap>> processor = buildProcessor(op, multiInletController, outletController);
        compoundProcessors.put(bounds, processor);
    }

    private Inlet<ConceptMap> createNextDownstream(ConceptMap answer) {
        // TODO: Maybe this is overcomplicated and we should use the registry instead. We should really consider this point.
        Resolvable<?> nextResolvable = null;  // TODO: Somehow we'll get the next resolvable
        // Get the Controller
        Driver<Controller<?, ?, ?, ?>> d = downstreamResolvers.computeIfAbsent(answer -> registry.registerController(nextResolvable));

        ConceptMap bounds = null;
        d.execute(actor -> actor.attachProcessor(bounds)); // TODO: Ideally block until receiving a response for this.

    }

    // TODO: Can't see a use for this abstraction yet
    class CompoundProcessorRef extends ProcessorRef<ConceptMap, ConceptMap, Inlet.DynamicMulti<ConceptMap>, Outlet.Single<ConceptMap>, InletController.DynamicMulti<ConceptMap>, OutletController.Single<ConceptMap>> {

        public CompoundProcessorRef(Driver<Processor<ConceptMap, ConceptMap, Inlet.DynamicMulti<ConceptMap>,
                Outlet.Single<ConceptMap>>> processorDriver, InletController.DynamicMulti<ConceptMap> inletController,
                                    OutletController.Single<ConceptMap> outletController) {
            super(processorDriver, inletController, outletController);
        }
    }

}
