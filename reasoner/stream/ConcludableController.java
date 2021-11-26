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
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.stream.Processor.Buffer;
import com.vaticle.typedb.core.reasoner.stream.Processor.Inlet;
import com.vaticle.typedb.core.reasoner.stream.Processor.InletManager;
import com.vaticle.typedb.core.reasoner.stream.Processor.Operation;
import com.vaticle.typedb.core.reasoner.stream.Processor.OutletManager;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConcludableController extends Controller<CONTROLLER_ID, ConceptMap, OUTPUT, PROCESSOR, ConcludableController> {

    private final Concludable concludable;
    private final ResolverRegistry registry;
    private final Set<Identifier.Variable.Retrievable> unboundVars;
    private final LinkedHashMap<Driver<ConclusionController>, Set<Unifier>> conclusionResolvers;

    protected ConcludableController(Driver<ConcludableController> driver,
                                    Concludable concludable, String name, ActorExecutorGroup executorService,
                                    ResolverRegistry registry) {
        super(driver, name, executorService);
        this.concludable = concludable;
        this.registry = registry;
        this.unboundVars = unboundVars();
        this.conclusionResolvers = initialiseDownstreamResolvers();
    }

    Set<Identifier.Variable.Retrievable> unboundVars() {
        return null;  // TODO
    }

    protected LinkedHashMap<Driver<ConclusionController>, Set<Unifier>> initialiseDownstreamResolvers() {
        return null;  // TODO: Requires `registry` and `concludable` only, which are already available
    }

//    protected List<ConclusionController> ruleDownstreams(ConceptMap bounds) {
//        return null; // TODO
//    }

    protected FunctionalIterator<Partial.Conclusion<?, ?>> processorsToRequest(Partial.Concludable<?> fromUpstream) {

        // TODO: somehow the processor's inletManager needs to request these new inlets one-by-one

        // TODO: This should be lazy and not know about the conclusionControllers.
        List<Partial.Conclusion<?, ?>> downstreams = new ArrayList<>();
        for (Map.Entry<Driver<ConclusionController>, Set<Unifier>> entry: conclusionResolvers.entrySet()) {
            Driver<ConclusionController> conclusionController = entry.getKey();
            Rule rule = registry.conclusionRule(conclusionController);
            for (Unifier unifier : entry.getValue()) {
                fromUpstream.toDownstream(unifier, rule).ifPresent(downstreams::add);
            }
        }
        return iterate(downstreams);
    }

    @Override
    protected InletManager<Partial.Conclusion<?, ?>> createInletManager(ConceptMap id) {
        Partial.Concludable<?> fromUpstream = null;
        FunctionalIterator<ConclusionReq> processorsToRequest =  processorsToRequest(fromUpstream);
//        Supplier<Inlet<Partial.Conclusion<?, ?>>> inletSupplier = inletSupplier(processorsToRequest);
        return new InletManager.DynamicMulti<>(processorsToRequest);
    }

    private Supplier<Inlet<Partial.Conclusion<?, ?>>> inletSupplier(FunctionalIterator<Partial.Conclusion<?,?>> processorsToRequest) {
        return null;
    }

    @Override
    protected OutletManager<ConceptMap> createOutletManager(ConceptMap id) {
        return null;
    }

    @Override
    protected Operation<Partial.Conclusion<?, ?>, ConceptMap> operation(ConceptMap bounds) {
        return defineOperation(bounds, unboundVars, () -> createTraversal(concludable.pattern(), bounds));
    }

    private static Operation<Partial.Conclusion<?, ?>, ConceptMap> defineOperation(ConceptMap bounds, Set<Identifier.Variable.Retrievable> unboundVars, Supplier<FunctionalIterator<ConceptMap>> traversalSupplier) {
        Operation<?, Partial.Conclusion<?, ?>> input = Operation.input();
        Operation<Partial.Conclusion<?, ?>, ConceptMap> downstreamOp = input.flatMap(a -> a.unifier().unUnify(a.conceptMap().concepts(), a.instanceRequirements()));   // TODO: if flatmapping produces an empty iterator for non-empty input then the pull() should be retried
        Source<ConceptMap> traversalSource = Source.fromIteratorSupplier(traversalSupplier);  // TODO: Delay opening the traversal until needed. Use a non-eager traversal wrapper.
        Operation<Partial.Conclusion<?, ?>, ConceptMap> op = Operation.sourceJoin(traversalSource, downstreamOp);  // TODO: Could be a fluent .join(). Perhaps instead this should be attached as one of the inlets? But the inlets and outlets are currently the actor boundaries, so maybe not.
        boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);  // Determines whether to use findFirst() or find all results
        if (singleAnswerRequired) {
            op = op.findFirst();  // Will finish the stream once one answer is found
        }
        Buffer<ConceptMap> buffer = new Buffer<>();  // TODO: Is the buffer ever needed here or only in the outlet? It can be needed for deduplication which is inefficient
        op = op.buffer(buffer);
        // TODO: toUpstreamLookup? Requires concludable to determine whether answer is inferred
        return op;
    }

}
