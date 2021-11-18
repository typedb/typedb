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

public class CompoundController {

    private void createCompoundProcessor(ConceptMap bounds) {
        // TODO: Change source and sink to refer to inlets and outlets (except the traversalSource which is actually a
        //  source). These can then be typed and we can keep a record of whether we can message children to add extra inlets or outlets.
        Processor.Buffer<ConceptMap> buffer = new Processor.Buffer<>();

        Processor.Operation<ConceptMap, Processor.Inlet<ConceptMap>> downstreamOp = Processor.Operation.input().flatMapOrRetry(a -> createNextDownstream(a));
//        Processor.Operation<ConceptMap, Inlet<ConceptMap>> traversalOp = Controller.Source.fromIterator(createTraversal(concludable, bounds)).asOperation();  // TODO: Delay opening the traversal until needed. Use a non-eager traversal wrapper.
//        Processor.Operation<ConceptMap, ConceptMap> op = Processor.Operation.orderedJoin(traversalOp, downstreamOp);  // TODO: Could be a fluent .join(). Perhaps instead this should be attached as one of the inlets? But the inlets and outlets are currently the actor boundaries, so maybe not.

        Controller.OutletController.DynamicMulti<OUTPUT> multiOutletController = Controller.OutletController.dynamicMulti();
        Controller.InletController.DynamicMulti<INPUT> multiInletController = Controller.InletController.dynamicMulti();
        Controller.ProcessorRef<INPUT, OUTPUT, Processor.Inlet<ConceptMap>, Processor.Outlet<ConceptMap>, Controller.InletController.DynamicMulti<INPUT>, Controller.OutletController.DynamicMulti<OUTPUT>> processor = buildProcessor(op, multiInletController, multiOutletController);
        compoundProcessors.put(bounds, processor);
    }

}
