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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.stream.ConclusionController.ConclusionAns;
import com.vaticle.typedb.core.reasoner.stream.ConclusionController.ConclusionProcessor;
import com.vaticle.typedb.core.reasoner.stream.Processor.InletManager.Single;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConcludableController extends Controller<Concludable, ConceptMap, ConcludableController.ConcludableAns, ConcludableController.ConcludableProcessor, ConcludableController> {

    private final LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions;
    private final Set<Identifier.Variable.Retrievable> unboundVars;

    protected ConcludableController(Driver<ConcludableController> driver, String name, Concludable id, ActorExecutorGroup executorService) {
        super(driver, name, id, executorService);
        this.upstreamConclusions = initialiseUpstreamConclusions();
        this.unboundVars = unboundVars();
    }

    private LinkedHashMap<Rule.Conclusion, Set<Unifier>> initialiseUpstreamConclusions() {
        return null;  // TODO
    }

    private Set<Identifier.Variable.Retrievable> unboundVars() {
        return null;  // TODO
    }

    @Override
    protected Function<Driver<ConcludableProcessor>, ConcludableProcessor> createProcessorFunc(ConceptMap bounds) {
        Supplier<FunctionalIterator<ConceptMap>> traversalSupplier = () -> traversalIterator(id().pattern(), bounds);
        boolean singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);
        return d -> new ConcludableProcessor(d, driver(), "", upstreamConclusions, traversalSupplier, singleAnswerRequired);
    }

    @Override
    <UPS_CID, UPS_PID, PACKET, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, PACKET, UPS_PROCESSOR, UPS_CONTROLLER>, UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> Driver<UPS_CONTROLLER> getControllerForId(UPS_CID id) {
        if (id instanceof Rule.Conclusion) {
            Driver<ConclusionController> conclusionController = null; // TODO: Fetch from registry using rule conclusion
            return (Driver<UPS_CONTROLLER>) conclusionController;  // TODO: Using instanceof requires that we do a casting.
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return null;  // TODO
    }

    public static class ConcludableAns {
        public ConcludableAns(ConceptMap conceptMap) {
            // TODO
        }
    }  // TODO: Some wrapper of answers from concludable resolution

    public static class ConcludableProcessor extends Processor<ConcludableAns, ConcludableProcessor> {
        private final Map<Rule.Conclusion, InletManager.DynamicMulti<ConclusionAns, ConclusionProcessor>> inletManagers;
        private final Map<InletManager<ConclusionAns, ConclusionProcessor>.Inlet, Unifier> unifiers;
        private final Map<Single<ConclusionAns, ConclusionProcessor>, Unifier.Requirements.Instance> instanceRequirements;
        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final boolean singleAnswerRequired;
        private final ConceptMap bounds;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver,
                                    Driver<ConcludableController> controller, String name,
                                    LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                                    boolean singleAnswerRequired) {
            super(driver, controller, name, new OutletManager.DynamicMulti<>(onPull()));
            this.bounds = null;
            this.instanceRequirements = null;  // TODO
            this.traversalSuppplier = traversalSuppplier;
            this.singleAnswerRequired = singleAnswerRequired;
            this.inletManagers = new HashMap<>();
            createInletManagers(upstreamConclusions);  // TODO: For each conclusion create an InletManager
            this.unifiers = null;  // TODO
        }

        private void createInletManagers(LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions) {
            upstreamConclusions.forEach((conclusion, unifiers) -> {
                InletManager.DynamicMulti<ConclusionAns, ConclusionProcessor> ruleInletManager = new InletManager.DynamicMulti<>();
                inletManagers.put(conclusion, ruleInletManager);
                unifiers.forEach(unifier -> {
                    unifier.unify(bounds).ifPresent(upstreamBounds -> {
                        // Create a new inlet, storing the unifier against it
                        InletManager<ConclusionAns, ConclusionProcessor>.Inlet newInlet = ruleInletManager.newInlet();
                        this.unifiers.put(newInlet, unifier);
                        requestConnection(new Connection.Builder<>(driver(), conclusion, upstreamBounds, newInlet));
                    });
                });
            });
        }

        private static Supplier<ConcludableAns> onPull() {
            return null;  // TODO
        }

        protected void operation() {
            inletManagers.values().forEach(im -> {
                Operation<?, ConclusionAns> input = Operation.input(im);

                // TODO: Actually we can arrive at the conclusion that we want multiple InletManagers, one for each rule,
                //  each having only one inlet. This is because we can compute all of them ahead of time, and we can keep
                //  hold of unifiers in a map with inletManagers, but we have no knowledge of the inlets inside the
                //  managers, so we can't hold a unifier per inlet.

                // TODO: The unifiers need to be held against each inlet actually so that we apply the right un-unification to each received answer
                Operation<ConclusionAns, ConceptMap> downstreamOp = input.flatMap(a -> iterate(unifiers.get(im)).flatMap(u -> u.unUnify(a.concepts(), instanceRequirements.get(im))));   // TODO: if flatmapping produces an empty iterator for non-empty input then the pull() should be retried
//                Operation<ConclusionAns, ConceptMap> downstreamOp = input.flatMap(a -> Operation.input(unifiers.get(im)).flatMap(u -> u.unUnify(a.concepts(), instanceRequirements.get(im))));   // TODO: if flatmapping produces an empty iterator for non-empty input then the pull() should be retried
                Source<ConceptMap> traversalSource = Source.fromIteratorSupplier(traversalSuppplier);  // TODO: Delay opening the traversal until needed. Use a non-eager traversal wrapper.
                Operation<ConclusionAns, ConceptMap> op = Operation.sourceJoin(traversalSource, downstreamOp);  // TODO: Could be a fluent .join(). Perhaps instead this should be attached as one of the inlets? But the inlets and outlets are currently the actor boundaries, so maybe not.
                if (singleAnswerRequired) op = op.findFirst();  // Will finish the stream once one answer is found
                Operation<ConclusionAns, ConcludableAns> finalOp = op.map(ConcludableAns::new);
                // TODO: toUpstreamLookup? Requires concludable to determine whether answer is inferred
                outletManager().feed(finalOp);
            });
        }

    }
}
