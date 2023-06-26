/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.answer.Mapping;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.Processor.ConcludableRequest;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.Processor.NegatedRequest;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.Processor.RetrievableRequest;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView.FilteredNegation;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView.FilteredRetrievable;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView.MappedConcludable;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.controller.ConcludableController.Processor.Match.withExplainable;

public abstract class ConjunctionController<
        CONTROLLER extends ConjunctionController<CONTROLLER, PROCESSOR>,
        PROCESSOR extends AbstractProcessor<ConceptMap, ConceptMap, ?, PROCESSOR>
        > extends AbstractController<
        ConceptMap, ConceptMap, ConceptMap, ConjunctionController.Request<?>, PROCESSOR, CONTROLLER
        > {

    private final Set<Resolvable<?>> resolvables;
    private final Map<Retrievable, FilteredRetrievable> retrievableControllers;
    private final Map<Concludable, MappedConcludable> concludableControllers;
    private final Map<Negated, FilteredNegation> negationControllers;
    final ResolvableConjunction conjunction;
    final Set<Variable.Retrievable> outputVariables;
    final Map<Set<Variable.Retrievable>, ConjunctionStreamPlan> plans;

    ConjunctionController(Driver<CONTROLLER> driver, ResolvableConjunction conjunction, Set<Variable.Retrievable> outputVariables, Context context) {
        super(driver, context, () -> ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")");
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.retrievableControllers = new HashMap<>();
        this.concludableControllers = new HashMap<>();
        this.negationControllers = new HashMap<>();
        this.outputVariables = outputVariables;
        this.plans = new ConcurrentHashMap<>();
    }

    @Override
    protected void setUpUpstreamControllers() {
        assert resolvables.isEmpty();
        resolvables.addAll(registry().logicManager().compile(conjunction));

        iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable).forEachRemaining(c -> {
            concludableControllers.put(c, registry().getOrCreateConcludable(c));
        });
        iterate(resolvables).filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(r -> {
            retrievableControllers.put(r, registry().createRetrievable(r));
        });
        iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated).forEachRemaining(negated -> {
            try {
                negationControllers.put(negated, registry().createNegation(negated, conjunction));
            } catch (TypeDBException e) {
                terminate(e);
            }
        });
    }

    ConjunctionStreamPlan getPlan(Set<Variable.Retrievable> bounds) {
        return plans.computeIfAbsent(bounds, inputBounds -> {
            Set<com.vaticle.typedb.core.pattern.variable.Variable> boundVariables = iterate(inputBounds).map(id -> conjunction.pattern().variable(id)).toSet();
            List<Resolvable<?>> plan = planner().getPlan(conjunction, boundVariables).plan();
            assert resolvables.size() == plan.size() && resolvables.containsAll(plan);
            return ConjunctionStreamPlan.create(plan, inputBounds, outputVariables);
        });
    }

    static ConceptMap merge(ConceptMap into, ConceptMap from) {
        Map<Variable.Retrievable, Concept> compounded = new HashMap<>(into.concepts());
        compounded.putAll(from.concepts());
        return new ConceptMap(compounded, into.explainables().merge(from.explainables()));
    }

    @Override
    public void routeConnectionRequest(Request<?> connectionRequest) {
        if (connectionRequest.isRetrievable()) {
            RetrievableRequest req = connectionRequest.asRetrievable();
            FilteredRetrievable controllerView = retrievableControllers.get(req.controllerId());
            controllerView.controller().execute(actor -> actor.establishProcessorConnection(req
                    .withMap(c -> merge(c, req.bounds()))
                    .withNewBounds(req.bounds().filter(controllerView.filter()))));
        } else if (connectionRequest.isConcludable()) {
            ConcludableRequest req = connectionRequest.asConcludable();
            MappedConcludable controllerView = concludableControllers.get(req.controllerId());
            Mapping mapping = Mapping.of(controllerView.mapping());
            ConceptMap newPID = mapping.transform(req.bounds());
            controllerView.controller().execute(actor -> actor.establishProcessorConnection(req
                    .withMap(mapping::unTransform)
                    .withMap(c -> remapExplainable(c, req.controllerId()))
                    .withNewBounds(newPID)));
        } else if (connectionRequest.isNegated()) {
            NegatedRequest req = connectionRequest.asNegated();
            FilteredNegation controllerView = negationControllers.get(req.controllerId());
            ConceptMap newPID = req.bounds().filter(controllerView.filter());
            controllerView.controller().execute(actor -> actor.establishProcessorConnection(req
                    .withMap(c -> merge(c, req.bounds()))
                    .withNewBounds(newPID)));
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private static ConceptMap remapExplainable(ConceptMap answer, Concludable concludable) {
        List<ConceptMap.Explainable> explainables = answer.explainables().iterator().toList();
        assert explainables.size() <= 1;
        if (explainables.isEmpty()) return answer;
        else return withExplainable(new ConceptMap(answer.concepts()), concludable);
    }

    static class Request<CONTROLLER_ID> extends AbstractRequest<CONTROLLER_ID, ConceptMap, ConceptMap> {
        Request(Reactive.Identifier inputPortId,
                Driver<? extends Processor<?>> inputPortProcessor, CONTROLLER_ID controller_id,
                ConceptMap conceptMap) {
            super(inputPortId, inputPortProcessor, controller_id, conceptMap);
        }

        boolean isRetrievable() {
            return false;
        }

        RetrievableRequest asRetrievable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        boolean isConcludable() {
            return false;
        }

        ConcludableRequest asConcludable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        boolean isNegated() {
            return false;
        }

        NegatedRequest asNegated() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

    }

    protected abstract static class Processor<PROCESSOR extends Processor<PROCESSOR>>
            extends AbstractProcessor<ConceptMap, ConceptMap, Request<?>, PROCESSOR> {

        final ConceptMap bounds;
        final ConjunctionStreamPlan plan;
        final Set<Variable.Retrievable> outputVariables;
        private final Map<ConjunctionStreamPlan, Map<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> compoundStreamRegistry;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends ConjunctionController<?, PROCESSOR>> controller,
                  Context context, ConceptMap bounds, ConjunctionStreamPlan conjunctionStreamPlan,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.bounds = bounds;
            this.outputVariables = controller.actor().outputVariables;
            this.plan = conjunctionStreamPlan;
            this.compoundStreamRegistry = new HashMap<>();
            context.perfCounters().conjunctionProcessors.add(1);
        }

        public class CompoundStream extends TransformationStream<ConceptMap, ConceptMap> {
            private final ConjunctionStreamPlan streamPlan;
            private final ConceptMap identifierBounds;
            private final ConjunctionController.Processor<?> processor;
            private final Map<Publisher<ConceptMap>, Integer> childIndex;

            CompoundStream(ConjunctionController.Processor<?> processor, ConjunctionStreamPlan streamPlan, ConceptMap identifierBounds) {
                super(processor, new SubscriberRegistry.Multi<>(), new PublisherRegistry.Multi<>());
                this.processor = processor;
                this.identifierBounds = identifierBounds;
                this.streamPlan = streamPlan;
                Publisher<ConceptMap> firstChild;
                if (streamPlan.isCompoundStreamPlan()) {
                    firstChild = spawnPlanElement(streamPlan.asCompoundStreamPlan().childAt(0), identifierBounds);
                } else {
                    firstChild = spawnPlanElement(streamPlan.asResolvablePlan(), identifierBounds);
                }
                firstChild.registerSubscriber(this);
                this.childIndex = new HashMap<>();
                this.childIndex.put(firstChild, 0);
                processor().context().perfCounters().compoundStreams.add(1);
            }

            @Override
            public Either<Publisher<ConceptMap>, Set<ConceptMap>> accept(Publisher<ConceptMap> publisher,
                                                                         ConceptMap packet) {
                context().perfCounters().compoundStreamAccepts.add(1);
                ConceptMap mergedPacket = merge(identifierBounds, packet);
                int nextChildIndex = childIndex.get(publisher) + 1;

                assert context().explainEnabled() || streamPlan.isResolvablePlan() || streamPlan.asCompoundStreamPlan().childAt(nextChildIndex - 1).isResolvablePlan() || (
                        iterate(packet.concepts().keySet()).allMatch(v ->
                                streamPlan.asCompoundStreamPlan().childAt(nextChildIndex - 1).outputs().contains(v) ||
                                        streamPlan.asCompoundStreamPlan().childAt(nextChildIndex - 1).extensions().contains(v)) &&
                                packet.concepts().keySet().containsAll(streamPlan.asCompoundStreamPlan().childAt(nextChildIndex - 1).outputs()) &&
                                packet.concepts().keySet().containsAll(streamPlan.asCompoundStreamPlan().childAt(nextChildIndex - 1).extensions()));

                if (streamPlan.isCompoundStreamPlan() && nextChildIndex < streamPlan.asCompoundStreamPlan().size()) {
                    Publisher<ConceptMap> follower = spawnPlanElement(streamPlan.asCompoundStreamPlan().childAt(nextChildIndex), mergedPacket);
                    childIndex.put(follower, nextChildIndex);
                    return Either.first(follower);
                } else {
                    return Either.second(set(filterOutputsWithExplainables(mergedPacket, streamPlan.outputs())));
                }
            }

            private Publisher<ConceptMap> spawnPlanElement(ConjunctionStreamPlan planElement, ConceptMap availableBounds) {
                ConceptMap extension = filterOutputsWithExplainables(availableBounds, planElement.extensions());
                ConceptMap identifiers = availableBounds.filter(planElement.identifiers());
                assert planElement.identifiers().size() == identifiers.concepts().size() && identifiers.concepts().keySet().containsAll(planElement.identifiers());
                Publisher<ConceptMap> publisher;
                if (planElement.isResolvablePlan()) {
                    publisher = spawnResolvableElement(planElement.asResolvablePlan(), identifiers);
                } else if (planElement.isCompoundStreamPlan()) {
                    publisher = spawnCompoundStream(planElement.asCompoundStreamPlan(), identifiers);
                } else throw TypeDBException.of(ILLEGAL_STATE);

                return extendWithBounds(publisher, extension);
            }

            private Publisher<ConceptMap> spawnCompoundStream(ConjunctionStreamPlan.CompoundStreamPlan toSpawn, ConceptMap mergedPacket) {
                ConceptMap identifyingBounds = mergedPacket.filter(toSpawn.identifiers());
                assert this.streamPlan.isCompoundStreamPlan();
                if (this.streamPlan.asCompoundStreamPlan().isExclusiveReaderOfChild(toSpawn) && !toSpawn.mayProduceDuplicates()) {
                    return new CompoundStream(processor, toSpawn, identifyingBounds)
                            .map(conceptMap -> filterOutputsWithExplainables(conceptMap, toSpawn.outputs()));
                } else {
                    return compoundStreamRegistry.computeIfAbsent(toSpawn, _x -> new HashMap<>()).computeIfAbsent(identifyingBounds, packet -> {
                        CompoundStream compoundStream = new CompoundStream(processor, toSpawn, identifyingBounds);
                        PoolingStream.BufferedFanStream<ConceptMap> bufferedStream = PoolingStream.BufferedFanStream.fanOut(compoundStream.processor);
                        compoundStream.map(conceptMap -> filterOutputsWithExplainables(conceptMap, toSpawn.outputs())).registerSubscriber(bufferedStream);
                        return bufferedStream;
                    });
                }
            }

            private Reactive.Publisher<ConceptMap> spawnResolvableElement(ConjunctionStreamPlan.ResolvablePlan toSpawn, ConceptMap carriedBounds) {
                InputPort<ConceptMap> input = createInputPort();
                Resolvable<?> resolvable = toSpawn.resolvable();
                ConceptMap identifiers = carriedBounds.filter(resolvable.retrieves());
                if (resolvable.isRetrievable()) {
                    requestConnection(new RetrievableRequest(input.identifier(), driver(), resolvable.asRetrievable(), identifiers));
                } else if (resolvable.isConcludable()) {
                    requestConnection(new ConcludableRequest(input.identifier(), driver(), resolvable.asConcludable(), identifiers));
                } else if (resolvable.isNegated()) {
                    requestConnection(new NegatedRequest(input.identifier(), driver(), resolvable.asNegated(), identifiers));
                } else {
                    throw TypeDBException.of(ILLEGAL_STATE);
                }

                return input.map(conceptMap -> merge(filterOutputsWithExplainables(conceptMap, toSpawn.outputs()), identifiers));
            }

            private Publisher<ConceptMap> extendWithBounds(Publisher<ConceptMap> s, ConceptMap extension) {
                return extension.concepts().isEmpty() ? s : s.map(conceptMap -> merge(conceptMap, extension));
            }

            private ConceptMap filterOutputsWithExplainables(ConceptMap packet, Set<Variable.Retrievable> toVariables) {
                return context().explainEnabled() ? packet : packet.filter(toVariables);
            }
        }

        public static class RetrievableRequest extends Request<Retrievable> {

            private RetrievableRequest(
                    Reactive.Identifier inputPortId,
                    Driver<? extends Processor<?>> inputPortProcessor, Retrievable controllerId,
                    ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

            @Override
            public boolean isRetrievable() {
                return true;
            }

            @Override
            public RetrievableRequest asRetrievable() {
                return this;
            }

        }

        static class ConcludableRequest extends Request<Concludable> {

            private ConcludableRequest(
                    Reactive.Identifier inputPortId,
                    Driver<? extends Processor<?>> inputPortProcessor, Concludable controllerId,
                    ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

            public boolean isConcludable() {
                return true;
            }

            public ConcludableRequest asConcludable() {
                return this;
            }

        }

        static class NegatedRequest extends Request<Negated> {

            private NegatedRequest(
                    Reactive.Identifier inputPortId,
                    Driver<? extends Processor<?>> inputPortProcessor, Negated controllerId, ConceptMap processorId
            ) {
                super(inputPortId, inputPortProcessor, controllerId, processorId);
            }

            public boolean isNegated() {
                return true;
            }

            public NegatedRequest asNegated() {
                return this;
            }

        }
    }

    public abstract static class ConjunctionStreamPlan {
        protected final Set<Variable.Retrievable> identifierVariables; // If the identifier variables match, the results will match
        protected final Set<Variable.Retrievable> extensionVariables; // The variables in mergeWithRemainingVars
        protected final Set<Variable.Retrievable> outputVariables;  // Strip out everything other than these.

        public ConjunctionStreamPlan(Set<Variable.Retrievable> identifierVariables, Set<Variable.Retrievable> extensionVariables, Set<Variable.Retrievable> outputVariables) {
            this.identifierVariables = identifierVariables;
            this.extensionVariables = extensionVariables;
            this.outputVariables = outputVariables;
        }

        public static ConjunctionStreamPlan create(List<Resolvable<?>> resolvableOrder, Set<Variable.Retrievable> inputVariables, Set<Variable.Retrievable> outputVariables) {
            Builder builder = new Builder(resolvableOrder, inputVariables, outputVariables);
            return builder.flatten(builder.build());
        }

        public static boolean isExclusiveReader(CompoundStreamPlan reader, CompoundStreamPlan read, Set<Variable.Retrievable> topLevelBounds) {
            return read.extensions().isEmpty() && read.identifierVariables.containsAll(Builder.VariableSets.difference(reader.identifierVariables, topLevelBounds));
        }

        public boolean isResolvablePlan() {
            return false;
        }

        public boolean isCompoundStreamPlan() {
            return false;
        }

        public ResolvablePlan asResolvablePlan() {
            throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(ResolvablePlan.class));
        }

        public CompoundStreamPlan asCompoundStreamPlan() {
            throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(CompoundStreamPlan.class));
        }

        public Set<Variable.Retrievable> outputs() {
            return outputVariables;
        }

        public Set<Variable.Retrievable> identifiers() {
            return identifierVariables;
        }

        public Set<Variable.Retrievable> extensions() {
            return extensionVariables;
        }

        public abstract boolean mayProduceDuplicates();

        public static class ResolvablePlan extends ConjunctionStreamPlan {
            private final Resolvable<?> resolvable;
            private final boolean mayProduceDuplicates;

            public ResolvablePlan(Resolvable<?> resolvable, Set<Variable.Retrievable> identifierVariables, Set<Variable.Retrievable> extendOutputWith, Set<Variable.Retrievable> outputVariables) {
                super(identifierVariables, extendOutputWith, outputVariables);
                this.resolvable = resolvable;
                this.mayProduceDuplicates = Builder.VariableSets.difference(resolvable.retrieves(), Builder.VariableSets.union(identifierVariables, extendOutputWith)).size() > 0;
            }

            @Override
            public boolean isResolvablePlan() {
                return true;
            }

            @Override
            public ResolvablePlan asResolvablePlan() {
                return this;
            }

            @Override
            public boolean mayProduceDuplicates() {
                return mayProduceDuplicates;
            }

            public Resolvable<?> resolvable() {
                return resolvable;
            }

            @Override
            public String toString() {
                return String.format("{[(%s), (%s), (%s)] :: Resolvable(%s)}",
                        String.join(", ", iterate(identifierVariables).map(Variable::toString).toList()),
                        String.join(", ", iterate(extensionVariables).map(Variable::toString).toList()),
                        String.join(", ", iterate(outputVariables).map(Variable::toString).toList()),
                        resolvable.toString()
                );
            }
        }

        public static class CompoundStreamPlan extends ConjunctionStreamPlan {
            private final List<ConjunctionStreamPlan> subPlans;
            private final boolean mayProduceDuplicates;
            private final Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild;

            public CompoundStreamPlan(List<ConjunctionStreamPlan> subPlans,
                                      Set<Variable.Retrievable> identifierVariables, Set<Variable.Retrievable> extendOutputWith, Set<Variable.Retrievable> outputVariables,
                                      Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild) {
                super(identifierVariables, extendOutputWith, outputVariables);
                assert subPlans.size() > 1;
                this.subPlans = subPlans;
                this.mayProduceDuplicates = subPlans.get(subPlans.size() - 1).isResolvablePlan() &&
                        subPlans.get(subPlans.size() - 1).asResolvablePlan().mayProduceDuplicates();
                this.isExclusiveReaderOfChild = isExclusiveReaderOfChild;
            }

            @Override
            public boolean isCompoundStreamPlan() {
                return true;
            }

            @Override
            public CompoundStreamPlan asCompoundStreamPlan() {
                return this;
            }

            @Override
            public boolean mayProduceDuplicates() {
                return mayProduceDuplicates;
            }

            public ConjunctionStreamPlan childAt(int i) {
                return subPlans.get(i);
            }

            public int size() {
                return subPlans.size();
            }

            @Override
            public String toString() {
                return String.format("{[(%s), (%s), (%s)] :: [%s]}",
                        String.join(", ", iterate(identifierVariables).map(Variable::toString).toList()),
                        String.join(", ", iterate(extensionVariables).map(Variable::toString).toList()),
                        String.join(", ", iterate(outputVariables).map(Variable::toString).toList()),
                        String.join(" ; ", iterate(subPlans).map(ConjunctionStreamPlan::toString).toList()));
            }

            public boolean isExclusiveReaderOfChild(ConjunctionStreamPlan child) {
                return isExclusiveReaderOfChild.getOrDefault(child, false);
            }
        }

        private static class Builder {
            private final List<Resolvable<?>> resolvables;
            private final Set<Variable.Retrievable> inputBounds;
            private final Set<Variable.Retrievable> outputBounds;

            private final List<Set<Variable.Retrievable>> boundsAt;

            private Builder(List<Resolvable<?>> resolvables, Set<Variable.Retrievable> inputBounds, Set<Variable.Retrievable> outputBounds) {
                this.resolvables = resolvables;
                this.inputBounds = inputBounds;
                this.outputBounds = outputBounds;
                boundsAt = new ArrayList<>();
                Set<Variable.Retrievable> runningBounds = new HashSet<>(inputBounds);
                for (Resolvable<?> resolvable : resolvables) {
                    boundsAt.add(new HashSet<>(runningBounds));
                    runningBounds.addAll(resolvable.retrieves());
                }
            }

            private ConjunctionStreamPlan build() {
                return buildRecursivePrefix(resolvables, inputBounds, outputBounds);
            }

            public ConjunctionStreamPlan buildRecursivePrefix(List<Resolvable<?>> subConjunction, Set<Variable.Retrievable> availableInputs, Set<Variable.Retrievable> requiredOutputs) {
                if (subConjunction.size() == 1) {
                    VariableSets variableSets = VariableSets.determineVariableSets(list(), subConjunction, availableInputs, requiredOutputs);
                    //  use resolvableOutputs instead of rightOutputs because this node has to do the job of the parent as well - joining the identifiers
                    Set<Variable.Retrievable> resolvableOutputs = VariableSets.difference(requiredOutputs, variableSets.extendOutputWith);
                    return new ResolvablePlan(subConjunction.get(0), variableSets.rightInputs, variableSets.extendOutputWith, resolvableOutputs);
                } else {
                    Pair<List<Resolvable<?>>, List<Resolvable<?>>> divided = divide(subConjunction);
                    VariableSets variableSets = VariableSets.determineVariableSets(divided.first(), divided.second(), availableInputs, requiredOutputs);
                    ConjunctionStreamPlan leftPlan = buildRecursivePrefix(divided.first(), variableSets.leftIdentifiers, variableSets.leftOutputs);
                    ConjunctionStreamPlan rightPlan = buildRecursiveSuffix(divided.second(), variableSets.rightInputs, variableSets.rightOutputs);

                    return new CompoundStreamPlan(list(leftPlan, rightPlan), variableSets.identifiers, variableSets.extendOutputWith, requiredOutputs, new HashMap<>());
                }
            }

            public ConjunctionStreamPlan buildRecursiveSuffix(List<Resolvable<?>> subConjunction, Set<Variable.Retrievable> availableInputs, Set<Variable.Retrievable> requiredOutputs) {
                if (subConjunction.size() == 1) {
                    VariableSets variableSets = VariableSets.determineVariableSets(list(), subConjunction, availableInputs, requiredOutputs);
                    Set<Variable.Retrievable> resolvableOutputs = VariableSets.difference(requiredOutputs, variableSets.extendOutputWith);
                    return new ResolvablePlan(subConjunction.get(0), variableSets.rightInputs, variableSets.extendOutputWith, resolvableOutputs);
                } else {
                    List<Resolvable<?>> suffix = subConjunction.subList(1, subConjunction.size());
                    VariableSets variableSets = VariableSets.determineVariableSets(subConjunction.subList(0, 1), subConjunction.subList(1, subConjunction.size()), availableInputs, requiredOutputs);
                    ConjunctionStreamPlan leftPlan = new ResolvablePlan(subConjunction.get(0), variableSets.leftIdentifiers, set(), variableSets.leftOutputs);
                    ConjunctionStreamPlan rightPlan = buildRecursiveSuffix(suffix, variableSets.rightInputs, variableSets.rightOutputs);
                    return new CompoundStreamPlan(list(leftPlan, rightPlan),
                            variableSets.identifiers, variableSets.extendOutputWith, requiredOutputs, new HashMap<>());
                }
            }

            public Pair<List<Resolvable<?>>, List<Resolvable<?>>> divide(List<Resolvable<?>> subConjunction) {
                int splitAfter;
                Set<Variable.Retrievable> subtreeVariables = new HashSet<>(subConjunction.get(subConjunction.size() - 1).retrieves());
                for (splitAfter = subConjunction.size() - 2; splitAfter > 0; splitAfter--) {
                    subtreeVariables.addAll(subConjunction.get(splitAfter).retrieves());
                    Set<Variable.Retrievable> subtreeBounds = VariableSets.intersection(boundsAt.get(splitAfter), subtreeVariables);
                    Set<Variable.Retrievable> candidateFirstChildBounds = VariableSets.intersection(subConjunction.get(splitAfter).retrieves(), boundsAt.get(splitAfter));

                    if (!VariableSets.difference(subtreeBounds, candidateFirstChildBounds).isEmpty()) {
                        break;
                    }
                }
                assert splitAfter >= 0;

                List<Resolvable<?>> left = new ArrayList<>();
                List<Resolvable<?>> right = new ArrayList<>();
                for (int j = 0; j < subConjunction.size(); j++) {
                    if (j <= splitAfter) left.add(subConjunction.get(j));
                    else right.add(subConjunction.get(j));
                }

                return new Pair<>(left, right);
            }

            private ConjunctionStreamPlan flatten(ConjunctionStreamPlan conjunctionStreamPlan) {
                if (conjunctionStreamPlan.isResolvablePlan()) {
                    return conjunctionStreamPlan;
                } else {
                    CompoundStreamPlan asCompoundStreamPlan = conjunctionStreamPlan.asCompoundStreamPlan();
                    assert asCompoundStreamPlan.size() == 2;
                    List<ConjunctionStreamPlan> subPlans = new ArrayList<>();
                    for (int i = 0; i < 2; i++) {
                        ConjunctionStreamPlan unflattenedChild = asCompoundStreamPlan.childAt(i);

                        if (canFlattenInto(asCompoundStreamPlan, unflattenedChild)) {
                            subPlans.addAll(flatten(unflattenedChild).asCompoundStreamPlan().subPlans);
                        } else {
                            subPlans.add(flatten(unflattenedChild));
                        }
                    }

                    Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild = new HashMap<>();
                    for (ConjunctionStreamPlan subPlan : subPlans) {
                        boolean exclusivelyReadsIthChild = subPlan.isResolvablePlan() ||  // We don't re-use resolvable plans
                                ConjunctionStreamPlan.isExclusiveReader(asCompoundStreamPlan, subPlan.asCompoundStreamPlan(), outputBounds);
                        isExclusiveReaderOfChild.put(subPlan, exclusivelyReadsIthChild);
                    }

                    return new CompoundStreamPlan(subPlans, asCompoundStreamPlan.identifiers(), asCompoundStreamPlan.extensions(), asCompoundStreamPlan.outputs(), isExclusiveReaderOfChild);
                }
            }

            private boolean canFlattenInto(CompoundStreamPlan conjunctionStreamPlan, ConjunctionStreamPlan unflattenedChild) {
                return unflattenedChild.isCompoundStreamPlan() &&
                        isExclusiveReader(conjunctionStreamPlan.asCompoundStreamPlan(), unflattenedChild.asCompoundStreamPlan(), inputBounds) &&
                        boundsRemainSatisfied(conjunctionStreamPlan.asCompoundStreamPlan(), unflattenedChild.asCompoundStreamPlan());
            }

            private static boolean boundsRemainSatisfied(CompoundStreamPlan conjunctionStreamPlan, CompoundStreamPlan childToFlatten) {
                return VariableSets.difference(
                                childToFlatten.childAt(1).identifierVariables,
                                VariableSets.union(conjunctionStreamPlan.identifierVariables, childToFlatten.childAt(0).outputVariables))
                        .isEmpty() &&
                        VariableSets.union(
                                        childToFlatten.asCompoundStreamPlan().childAt(1).outputs(),
                                        childToFlatten.asCompoundStreamPlan().childAt(1).extensions())
                                .equals(childToFlatten.outputs());
            }

            private static class VariableSets {

                public final Set<Variable.Retrievable> identifiers;
                public final Set<Variable.Retrievable> extendOutputWith;
                public final Set<Variable.Retrievable> requiredOutputs;
                public final Set<Variable.Retrievable> leftIdentifiers;
                public final Set<Variable.Retrievable> leftOutputs;
                public final Set<Variable.Retrievable> rightInputs;
                public final Set<Variable.Retrievable> rightOutputs;

                public VariableSets(Set<Variable.Retrievable> identifiers, Set<Variable.Retrievable> extendOutputWith, Set<Variable.Retrievable> requiredOutputs,
                                    Set<Variable.Retrievable> leftIdentifiers, Set<Variable.Retrievable> leftOutputs,
                                    Set<Variable.Retrievable> rightInputs, Set<Variable.Retrievable> rightOutputs) {
                    this.identifiers = identifiers;
                    this.extendOutputWith = extendOutputWith;
                    this.requiredOutputs = requiredOutputs;
                    this.leftIdentifiers = leftIdentifiers;
                    this.leftOutputs = leftOutputs;
                    this.rightInputs = rightInputs;
                    this.rightOutputs = rightOutputs;
                }

                private static VariableSets determineVariableSets(List<Resolvable<?>> left, List<Resolvable<?>> right, Set<Variable.Retrievable> availableInputs, Set<Variable.Retrievable> requiredOutputs) {
                    Set<Variable.Retrievable> leftVariables = iterate(left).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
                    Set<Variable.Retrievable> rightVariables = iterate(right).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
                    Set<Variable.Retrievable> allUsedVariables = union(leftVariables, rightVariables);

                    Set<Variable.Retrievable> identifiers = intersection(availableInputs, allUsedVariables);
                    Set<Variable.Retrievable> extendOutputWith = difference(availableInputs, allUsedVariables);
                    Set<Variable.Retrievable> rightOutputs = difference(requiredOutputs, availableInputs);

                    Set<Variable.Retrievable> leftIdentifiers = intersection(identifiers, leftVariables);
                    Set<Variable.Retrievable> rightInputs = intersection(union(identifiers, leftVariables), union(rightVariables, rightOutputs));
                    Set<Variable.Retrievable> leftOutputs = difference(rightInputs, difference(identifiers, leftIdentifiers));

                    return new VariableSets(identifiers, extendOutputWith, requiredOutputs, leftIdentifiers, leftOutputs, rightInputs, rightOutputs);
                }

                public static Set<Variable.Retrievable> union(Set<Variable.Retrievable> a, Set<Variable.Retrievable> b) {
                    Set<Variable.Retrievable> result = new HashSet<>(a);
                    result.addAll(b);
                    return result;
                }

                private static Set<Variable.Retrievable> intersection(Set<Variable.Retrievable> a, Set<Variable.Retrievable> b) {
                    Set<Variable.Retrievable> result = new HashSet<>(a);
                    result.retainAll(b);
                    return result;
                }

                private static Set<Variable.Retrievable> difference(Set<Variable.Retrievable> a, Set<Variable.Retrievable> b) {
                    Set<Variable.Retrievable> result = new HashSet<>(a);
                    result.removeAll(b);
                    return result;
                }
            }
        }
    }
}
