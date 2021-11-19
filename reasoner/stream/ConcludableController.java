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
import com.vaticle.typedb.core.reasoner.stream.Processor.Buffer;
import com.vaticle.typedb.core.reasoner.stream.Processor.Inlet;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;
import com.vaticle.typedb.core.reasoner.stream.Processor.Outlet;

import java.util.List;
import java.util.Map;

public class ConcludableController extends Controller {
    Map<ConceptMap, ProcessorRef<>> concludableProcessors;

    protected ConcludableController(Driver<Controller> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name, executorService);
    }

    private void createConcludableProcessor(ConceptMap bounds) {
        // TODO: Change source and sink to refer to inlets and outlets (except the traversalSource which is actually a
        //  source). These can then be typed and we can keep a record of whether we can message children to add extra inlets or outlets.
        Buffer<ConceptMap> buffer = new Buffer<>();

        Operation<ConceptMap, ConceptMap> downstreamOp = Operation.input().flatMapOrRetry(a -> a.unUnify(a.conceptMap(), requirements));
        Operation<ConceptMap, ConceptMap> traversalOp = Source.fromIterator(createTraversal(concludable, bounds)).asOperation();  // TODO: Delay opening the traversal until needed. Use a non-eager traversal wrapper.
        Operation<ConceptMap, ConceptMap> op = Operation.orderedJoin(traversalOp, downstreamOp);  // TODO: Could be a fluent .join(). Perhaps instead this should be attached as one of the inlets? But the inlets and outlets are currently the actor boundaries, so maybe not.
        boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars());  // Determines whether to use findFirst() or find all results
        if (singleAnswerRequired) {
            op = op.findFirst();  // Will finish the stream once one answer is found
        }
        op = op.buffer(buffer);
        // TODO: toUpstreamLookup? Requires concludable to determine whether answer is inferred

        OutletController.DynamicMulti<ConceptMap> multiOutletController = OutletController.dynamicMulti();
        InletController.DynamicMulti<ConceptMap> multiInletController = InletController.dynamicMulti();
        ProcessorRef<ConceptMap, ConceptMap, Inlet.DynamicMulti<ConceptMap>, Outlet.DynamicMulti<ConceptMap>, InletController.DynamicMulti<ConceptMap>, OutletController.DynamicMulti<ConceptMap>> processor = buildProcessor(op, multiInletController, multiOutletController);
        concludableProcessors.put(bounds, processor);
    }

    protected List<Stream> ruleDownstreams(ConceptMap bounds) {}


}
