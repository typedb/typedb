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
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.stream.ConclusionController.ConclusionAns;
import com.vaticle.typedb.core.reasoner.stream.ConclusionController.ConclusionProcessor;
import com.vaticle.typedb.core.reasoner.stream.Processor.InletManager.DynamicMulti;
import com.vaticle.typedb.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ConcludableController extends Controller<Concludable, ConceptMap, ConcludableController.ConcludableAns, ConcludableController.ConcludableProcessor, ConcludableController> {

    private final UpstreamTransceiver<Rule.Conclusion, ConceptMap, ConclusionAns, ConclusionController, ConclusionProcessor> conclusionHandler;
    private final LinkedHashMap<Rule.Conclusion, Set<Unifier>> upstreamConclusions;
    private final Set<Identifier.Variable.Retrievable> unboundVars;

    protected ConcludableController(Driver<ConcludableController> driver, String name, Concludable id, ActorExecutorGroup executorService) {
        super(driver, name, id, executorService);
        this.upstreamConclusions = initialiseUpstreamConclusions();
        this.conclusionHandler = new ConclusionHandler();
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
        return d -> new ConcludableProcessor(d, driver(), "", traversalSupplier, singleAnswerRequired);
    }

    private FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return null;  // TODO
    }

    @Override
    protected <UPS_CID, UPS_PID, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, UPS_PROCESSOR, UPS_CONTROLLER>,
            UPS_PROCESSOR extends Processor<?, UPS_PROCESSOR>
            > UpstreamTransceiver<UPS_CID, UPS_PID, ?, UPS_CONTROLLER, UPS_PROCESSOR> getUpstreamTransceiver(
                    UPS_CID id, @Nullable Driver<UPS_CONTROLLER> controller) {
        if (id instanceof Rule.Conclusion) {
            return (UpstreamTransceiver<UPS_CID, UPS_PID, ?, UPS_CONTROLLER, UPS_PROCESSOR>) conclusionHandler;  // TODO: Using instanceof requires that we do a casting. Ideally we would avoid this.
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    class ConclusionHandler extends UpstreamTransceiver<Rule.Conclusion, ConceptMap, ConclusionAns, ConclusionController, ConclusionProcessor> {

        @Override
        protected Driver<ConclusionController> getControllerForId(Rule.Conclusion id) {
            return null;  // TODO: Go to the registry and get it
        }
    }

    public static class ConcludableAns {}  // TODO: Some wrapper of answers from concludable resolution

    public static class ConcludableProcessor extends Processor<ConcludableAns, ConcludableProcessor> {
        private final Map<Rule.Conclusion, DynamicMulti<Rule.Conclusion, ConclusionAns, ConclusionProcessor>> inletManagers;
        private final Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier;
        private final boolean singleAnswerRequired;

        public ConcludableProcessor(Driver<ConcludableProcessor> driver,
                                    Driver<ConcludableController> controller, String name,
                                    Supplier<FunctionalIterator<ConceptMap>> traversalSuppplier,
                                    boolean singleAnswerRequired) {
            super(driver, controller, name, new Processor.OutletManager.DynamicMulti());
            this.traversalSuppplier = traversalSuppplier;
            this.singleAnswerRequired = singleAnswerRequired;
            this.inletManagers = null;  // TODO: For each conclusion create an InletManager
        }

        @Override
        public <INLET_MANAGER_ID, INLET_ID, INPUT, UPS_PROCESSOR extends Processor<INPUT, UPS_PROCESSOR>
                > InletManager<INLET_ID, INPUT, UPS_PROCESSOR> getInletManager(INLET_MANAGER_ID inletManagerId) {
            if (inletManagerId instanceof Rule.Conclusion) {
                return (DynamicMulti<INLET_ID, INPUT, UPS_PROCESSOR>) inletManagers.get(inletManagerId);
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        protected Operation<ConclusionAns, ConceptMap> operation() {
            return defineOperation(singleAnswerRequired, traversalSuppplier);
        }

        private static Operation<ConclusionAns, ConceptMap> defineOperation(boolean singleAnswerRequired, Supplier<FunctionalIterator<ConceptMap>> traversalSupplier) {
            Operation<?, AnswerState.Partial.Conclusion<?, ?>> input = Operation.input();
//            Operation<ConclusionAns, ConceptMap> downstreamOp = input.flatMap(a -> a.unifier().unUnify(a.conceptMap().concepts(), a.instanceRequirements()));   // TODO: if flatmapping produces an empty iterator for non-empty input then the pull() should be retried
            Operation<ConclusionAns, ConceptMap> downstreamOp = null; // TODO: Use roughly the above
            Processor.Source<ConceptMap> traversalSource = Processor.Source.fromIteratorSupplier(traversalSupplier);  // TODO: Delay opening the traversal until needed. Use a non-eager traversal wrapper.
            Operation<ConclusionAns, ConceptMap> op = Operation.sourceJoin(traversalSource, downstreamOp);  // TODO: Could be a fluent .join(). Perhaps instead this should be attached as one of the inlets? But the inlets and outlets are currently the actor boundaries, so maybe not.
            if (singleAnswerRequired) op = op.findFirst();  // Will finish the stream once one answer is found
            // TODO: toUpstreamLookup? Requires concludable to determine whether answer is inferred
            return op;
        }
    }
}
