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

public class ConcludableController extends Controller<Concludable, ConceptMap, ConcludableController.ConcludableAns,
        ConcludableController.ConcludableProcessor, ConcludableController> {

    private final LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions;
    private final Set<Identifier.Variable.Retrievable> unboundVars;

    protected ConcludableController(Driver<ConcludableController> driver, String name, Concludable id,
                                    ActorExecutorGroup executorService) {
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
        return d -> new ConcludableProcessor(d, driver(), "", bounds, unboundVars, upstreamConclusions,
                                             () -> traversalIterator(id().pattern(), bounds));
    }

    @Override
    <UPS_CID, UPS_PID, PACKET,
            UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, PACKET, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<PACKET, UPS_PROCESSOR>> Driver<UPS_CONTROLLER> getControllerForId(UPS_CID id) {
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
    }

    public static class ConcludableProcessor extends Processor<ConcludableAns, ConcludableProcessor> {

        private final ConceptMap bounds;
        private final Map<Rule.Conclusion, InletManager.DynamicMulti<ConclusionAns, ConclusionProcessor>> inletManagers;
        private final Map<InletManager<ConclusionAns, ConclusionProcessor>.Inlet, Unifier> inletUnifiers;
        private final Map<Single<ConclusionAns, ConclusionProcessor>, Unifier.Requirements.Instance> instanceRequirements;
        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final boolean singleAnswerRequired;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver,
                                    Driver<ConcludableController> controller, String name,
                                    ConceptMap bounds, Set<Identifier.Variable.Retrievable> unboundVars,
                                    LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier) {
            super(driver, controller, name, new OutletManager.DynamicMulti<>(onPull()));
            this.bounds = bounds;
            this.instanceRequirements = null;  // TODO
            this.traversalSuppplier = traversalSuppplier;
            this.singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);;
            this.inletManagers = new HashMap<>();
            this.inletUnifiers = new HashMap<>();
            createInletManagersAndInletUnifiers(upstreamConclusions);  // TODO: For each conclusion create an InletManager
        }

        private void createInletManagersAndInletUnifiers(LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions) {
            upstreamConclusions.forEach((conclusion, unifiers) -> {
                InletManager.DynamicMulti<ConclusionAns, ConclusionProcessor> ruleInletManager = new InletManager.DynamicMulti<>();
                inletManagers.put(conclusion, ruleInletManager);
                unifiers.forEach(unifier -> {
                    unifier.unify(bounds).ifPresent(upstreamBounds -> {
                        // Create a new inlet, storing the unifier against it
                        InletManager<ConclusionAns, ConclusionProcessor>.Inlet newInlet = ruleInletManager.newInlet();
                        this.inletUnifiers.put(newInlet, unifier);
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
                im.inlets().forEach(inlet -> {
                    ReactiveTransform<?, ConclusionAns> input = ReactiveTransform.input(im);
//                    ReactiveTransform<ConclusionAns, ConceptMap> downstreamOp = input.flatMap(a -> ReactiveTransform.fromIterator(inletUnifiers.get(im).unUnify(a.concepts(), instanceRequirements.get(im))));
                    ReactiveTransform<ConclusionAns, ConceptMap> downstreamOp = null;
                    Source<ConceptMap> traversalSource = Source.fromIteratorSupplier(traversalSuppplier);
                    ReactiveTransform<ConclusionAns, ConceptMap> op = ReactiveTransform.sourceJoin(traversalSource, downstreamOp);
                    if (singleAnswerRequired) op = op.findFirst();
                    ReactiveTransform<ConclusionAns, ConcludableAns> finalOp = op.map(ConcludableAns::new);
                    // TODO: toUpstreamLookup? Requires concludable to determine whether answer is inferred
                    outletManager().feed(finalOp);
                });
            });
        }

    }
}
