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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.stream.ConclusionController.ConclusionAns;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
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

        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final boolean singleAnswerRequired;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver,
                                    Driver<ConcludableController> controller, String name,
                                    ConceptMap bounds, Set<Identifier.Variable.Retrievable> unboundVars,
                                    LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier) {
            super(driver, controller, name, new OutletManager.DynamicMulti<>());
            this.traversalSuppplier = traversalSuppplier;
            this.singleAnswerRequired = bounds.concepts().keySet().containsAll(unboundVars);

            Source<ConceptMap> traversalSource = Source.fromIteratorSupplier(traversalSuppplier);  // TODO: We think we'd like a way to prioritise this upstream over others
//            Function<ConceptMap, FunctionalIterator<ConcludableAns>> fn = c -> Iterators.single(new ConcludableAns(c));
//            outletManager()
//                    .findFirst()
//                    .flatMap(fn)
//                    .join(traversalSource);

            Reactive<ConceptMap, ?> op = new FlatMapReactive<>(
                    set(outletManager()), set(traversalSource),
                    conceptMap -> Iterators.single(new ConcludableAns(conceptMap))
            );

            if (singleAnswerRequired) op = op.findFirst();  // TODO: How do we achieve this? Needs to prevent any further pulls upon first result found

            Reactive<ConceptMap, ?> finalOp = op;
            upstreamConclusions.forEach((conclusion, unifiers) -> {
                Inlet.DynamicMultiPort<ConclusionAns, ConceptMap> ruleInlet = new Inlet.DynamicMultiPort<>(finalOp, set());
                finalOp.addUpstream(ruleInlet);  // TODO: We have to manually add this which isn't nice
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    // Create a new port, storing the unifier against it
                    FlatMapReactive<ConclusionAns, ConceptMap> newInlet = ruleInlet.newPort(conclusionAns -> unifier.unUnify(conclusionAns.concepts(), boundsAndRequirements.second()));
                    Connection.Builder<
                            ConclusionAns, ConcludableProcessor, Rule.Conclusion,ConceptMap,
                            ConclusionController.ConclusionProcessor
                            > builder = new Connection.Builder<>(driver(), conclusion, boundsAndRequirements.first(), newInlet);
                    requestConnection(builder);
                }));
            });
        }

    }
}
