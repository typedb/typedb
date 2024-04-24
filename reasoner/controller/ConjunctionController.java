/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.Collections;
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

import static com.vaticle.typedb.common.collection.Collections.concatToSet;
import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.controller.ConcludableController.Processor.Match.withExplainable;
import static java.util.Collections.emptySet;

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

        iterate(resolvables).filter(Resolvable::isConcludable).map(Resolvable::asConcludable).forEachRemaining(c ->
                concludableControllers.put(c, registry().getOrCreateConcludable(c))
        );
        iterate(resolvables).filter(Resolvable::isRetrievable).map(Resolvable::asRetrievable).forEachRemaining(r ->
                retrievableControllers.put(r, registry().createRetrievable(r))
        );
        iterate(resolvables).filter(Resolvable::isNegated).map(Resolvable::asNegated).forEachRemaining(negated ->
                negationControllers.put(negated, registry().createNegation(negated, conjunction))
        );
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
        private final Map<ConjunctionStreamPlan, Map<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> compoundStreamRegistry;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends ConjunctionController<?, PROCESSOR>> controller,
                  Context context, ConceptMap bounds, ConjunctionStreamPlan conjunctionStreamPlan,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.bounds = bounds;
            this.plan = conjunctionStreamPlan;
            this.compoundStreamRegistry = new HashMap<>();
            context.perfCounters().conjunctionProcessors.add(1);
        }

        public class CompoundStream extends TransformationStream<ConceptMap, ConceptMap> {

            private final ConjunctionController.Processor<?> processor;
            private final ConceptMap bounds;
            private final ConjunctionStreamPlan plan;
            private final Map<Publisher<ConceptMap>, Integer> childIndex;

            CompoundStream(ConjunctionController.Processor<?> processor, ConjunctionStreamPlan plan, ConceptMap bounds) {
                super(processor, new SubscriberRegistry.Multi<>(), new PublisherRegistry.Multi<>());
                this.processor = processor;
                this.bounds = bounds;
                this.plan = plan;
                this.childIndex = new HashMap<>();
                Publisher<ConceptMap> firstChild;
                if (plan.isCompoundStreamPlan()) {
                    firstChild = spawnPlanElement(plan.asCompoundStreamPlan().childAt(0), bounds);
                } else {
                    firstChild = spawnPlanElement(plan.asResolvablePlan(), bounds);
                }
                this.childIndex.put(firstChild, 0);
                firstChild.registerSubscriber(this);
                processor().context().perfCounters().compoundStreams.add(1);
            }

            @Override
            public Either<Publisher<ConceptMap>, Set<ConceptMap>> accept(Publisher<ConceptMap> publisher,
                                                                         ConceptMap packet) {
                context().perfCounters().compoundStreamMessagesReceived.add(1);
                ConceptMap mergedPacket = merge(bounds, packet);
                int nextChildIndex = childIndex.get(publisher) + 1;

                assert context().explainEnabled() || plan.isResolvablePlan() || plan.asCompoundStreamPlan().childAt(nextChildIndex - 1).isResolvablePlan() || (
                        iterate(packet.concepts().keySet()).allMatch(v ->
                                plan.asCompoundStreamPlan().childAt(nextChildIndex - 1).outputs().contains(v) ||
                                        plan.asCompoundStreamPlan().childAt(nextChildIndex - 1).extensions().contains(v)) &&
                                packet.concepts().keySet().containsAll(plan.asCompoundStreamPlan().childAt(nextChildIndex - 1).outputs()) &&
                                packet.concepts().keySet().containsAll(plan.asCompoundStreamPlan().childAt(nextChildIndex - 1).extensions()));

                if (plan.isCompoundStreamPlan() && nextChildIndex < plan.asCompoundStreamPlan().size()) {
                    Publisher<ConceptMap> follower = spawnPlanElement(plan.asCompoundStreamPlan().childAt(nextChildIndex), mergedPacket);
                    childIndex.put(follower, nextChildIndex);
                    return Either.first(follower);
                } else {
                    return Either.second(set(filterWithExplainables(mergedPacket, plan.outputs())));
                }
            }

            private Publisher<ConceptMap> spawnPlanElement(ConjunctionStreamPlan planElement, ConceptMap availableBounds) {
                ConceptMap extension = filterWithExplainables(availableBounds, planElement.extensions());
                ConceptMap planElementBounds = availableBounds.filter(planElement.identifiers());
                Publisher<ConceptMap> publisher;
                if (planElement.isResolvablePlan()) {
                    publisher = spawnResolvable(planElement.asResolvablePlan(), planElementBounds);
                } else if (planElement.isCompoundStreamPlan()) {
                    publisher = spawnCompoundStream(planElement.asCompoundStreamPlan(), planElementBounds);
                } else throw TypeDBException.of(ILLEGAL_STATE);

                return extendWithBounds(publisher, extension);
            }

            private Publisher<ConceptMap> spawnCompoundStream(ConjunctionStreamPlan.CompoundStreamPlan planElement, ConceptMap compoundStreamBounds) {
                if (this.plan.asCompoundStreamPlan().mustBufferAnswers(planElement)) {
                    return compoundStreamRegistry.computeIfAbsent(planElement, _x -> new HashMap<>()).computeIfAbsent(compoundStreamBounds, packet -> {
                        CompoundStream compoundStream = new CompoundStream(processor, planElement, compoundStreamBounds);
                        PoolingStream.BufferedFanStream<ConceptMap> bufferedStream = PoolingStream.BufferedFanStream.fanOut(compoundStream.processor);
                        compoundStream.map(conceptMap -> filterWithExplainables(conceptMap, planElement.outputs())).registerSubscriber(bufferedStream);
                        return bufferedStream;
                    });
                } else {
                    return new CompoundStream(processor, planElement, compoundStreamBounds)
                            .map(conceptMap -> filterWithExplainables(conceptMap, planElement.outputs()));
                }
            }

            private Reactive.Publisher<ConceptMap> spawnResolvable(ConjunctionStreamPlan.ResolvablePlan planElement, ConceptMap resolvableBounds) {
                InputPort<ConceptMap> input = createInputPort();
                Resolvable<?> resolvable = planElement.resolvable();
                if (resolvable.isRetrievable()) {
                    requestConnection(new RetrievableRequest(input.identifier(), driver(), resolvable.asRetrievable(), resolvableBounds));
                } else if (resolvable.isConcludable()) {
                    requestConnection(new ConcludableRequest(input.identifier(), driver(), resolvable.asConcludable(), resolvableBounds));
                } else if (resolvable.isNegated()) {
                    requestConnection(new NegatedRequest(input.identifier(), driver(), resolvable.asNegated(), resolvableBounds));
                } else {
                    throw TypeDBException.of(ILLEGAL_STATE);
                }

                return input.map(conceptMap -> merge(filterWithExplainables(conceptMap, planElement.outputs()), resolvableBounds));
            }

            private Publisher<ConceptMap> extendWithBounds(Publisher<ConceptMap> stream, ConceptMap extension) {
                return extension.concepts().isEmpty() ? stream : stream.map(conceptMap -> merge(conceptMap, extension));
            }

            private ConceptMap filterWithExplainables(ConceptMap packet, Set<Variable.Retrievable> filter) {
                return context().explainEnabled() ? packet : packet.filter(filter);
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
        protected final Set<Variable.Retrievable> identifierVariables;
        protected final Set<Variable.Retrievable> extensionVariables;
        protected final Set<Variable.Retrievable> outputVariables;

        public ConjunctionStreamPlan(Set<Variable.Retrievable> identifierVariables, Set<Variable.Retrievable> extensionVariables, Set<Variable.Retrievable> outputVariables) {
            this.identifierVariables = identifierVariables;
            this.extensionVariables = extensionVariables;
            this.outputVariables = outputVariables;
        }

        public static ConjunctionStreamPlan create(List<Resolvable<?>> resolvableOrder, Set<Variable.Retrievable> inputVariables, Set<Variable.Retrievable> outputVariables) {
            Builder builder = new Builder(resolvableOrder, inputVariables, outputVariables);
            return builder.flatten(builder.build());
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
                this.mayProduceDuplicates = !concatToSet(identifierVariables, extendOutputWith).containsAll(resolvable.retrieves());
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
            private final List<ConjunctionStreamPlan> childPlan;
            private final boolean mayProduceDuplicates;
            private final Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild;

            public CompoundStreamPlan(List<ConjunctionStreamPlan> childPlan,
                                      Set<Variable.Retrievable> identifierVariables, Set<Variable.Retrievable> extendOutputWith,
                                      Set<Variable.Retrievable> outputVariables,
                                      Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild) {
                super(identifierVariables, extendOutputWith, outputVariables);
                assert childPlan.size() > 1;
                this.childPlan = childPlan;
                this.mayProduceDuplicates = childPlan.get(childPlan.size() - 1).isResolvablePlan() &&
                        childPlan.get(childPlan.size() - 1).asResolvablePlan().mayProduceDuplicates();
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
                return childPlan.get(i);
            }

            public int size() {
                return childPlan.size();
            }

            @Override
            public String toString() {
                return String.format("{[(%s), (%s), (%s)] :: [%s]}",
                        String.join(", ", iterate(identifierVariables).map(Variable::toString).toList()),
                        String.join(", ", iterate(extensionVariables).map(Variable::toString).toList()),
                        String.join(", ", iterate(outputVariables).map(Variable::toString).toList()),
                        String.join(" ; ", iterate(childPlan).map(ConjunctionStreamPlan::toString).toList()));
            }

            public boolean mustBufferAnswers(ConjunctionStreamPlan child) {
                return child.mayProduceDuplicates() || !isExclusiveReaderOfChild(child);
            }

            private boolean isExclusiveReaderOfChild(ConjunctionStreamPlan child) {
                return isExclusiveReaderOfChild.getOrDefault(child, false);
            }
        }

        private static class Builder {
            private final List<Resolvable<?>> resolvables;
            private final Set<Variable.Retrievable> processorInputs;
            private final Set<Variable.Retrievable> processorOutputs;

            private final List<Set<Variable.Retrievable>> boundsBefore;

            private Builder(List<Resolvable<?>> resolvables, Set<Variable.Retrievable> processorInputs, Set<Variable.Retrievable> processorOutputs) {
                this.resolvables = resolvables;
                this.processorInputs = processorInputs;
                this.processorOutputs = processorOutputs;
                boundsBefore = new ArrayList<>();
                Set<Variable.Retrievable> runningBounds = new HashSet<>(processorInputs);
                for (Resolvable<?> resolvable : resolvables) {
                    boundsBefore.add(new HashSet<>(runningBounds));
                    runningBounds.addAll(resolvable.retrieves());
                }
            }

            private ConjunctionStreamPlan build() {
                return buildPrefix(resolvables, processorInputs, processorOutputs);
            }

            public ConjunctionStreamPlan buildPrefix(List<Resolvable<?>> prefix, Set<Variable.Retrievable> availableInputs, Set<Variable.Retrievable> requiredOutputs) {
                if (prefix.size() == 1) {
                    VariableSets variableSets = VariableSets.create(list(), prefix, availableInputs, requiredOutputs);
                    //  use resolvableOutputs instead of rightOutputs because this node has to do the job of the parent as well - joining the identifiers
                    Set<Variable.Retrievable> resolvableOutputs = difference(requiredOutputs, variableSets.extensions);
                    return new ResolvablePlan(prefix.get(0), variableSets.rightInputs, variableSets.extensions, resolvableOutputs);
                } else {
                    Pair<List<Resolvable<?>>, List<Resolvable<?>>> divided = divide(prefix);
                    VariableSets variableSets = VariableSets.create(divided.first(), divided.second(), availableInputs, requiredOutputs);
                    ConjunctionStreamPlan leftPlan = buildPrefix(divided.first(), variableSets.leftIdentifiers, variableSets.leftOutputs);
                    ConjunctionStreamPlan rightPlan = buildSuffix(divided.second(), variableSets.rightInputs, variableSets.rightOutputs);
                    return new CompoundStreamPlan(list(leftPlan, rightPlan), variableSets.identifiers, variableSets.extensions, requiredOutputs, new HashMap<>());
                }
            }

            public ConjunctionStreamPlan buildSuffix(List<Resolvable<?>> suffix, Set<Variable.Retrievable> availableInputs, Set<Variable.Retrievable> requiredOutputs) {
                if (suffix.size() == 1) {
                    VariableSets variableSets = VariableSets.create(list(), suffix, availableInputs, requiredOutputs);
                    Set<Variable.Retrievable> resolvableOutputs = difference(requiredOutputs, variableSets.extensions);
                    return new ResolvablePlan(suffix.get(0), variableSets.rightInputs, variableSets.extensions, resolvableOutputs);
                } else {
                    List<Resolvable<?>> nextSuffix = suffix.subList(1, suffix.size());
                    VariableSets variableSets = VariableSets.create(suffix.subList(0, 1), suffix.subList(1, suffix.size()), availableInputs, requiredOutputs);
                    ConjunctionStreamPlan leftPlan = new ResolvablePlan(suffix.get(0), variableSets.leftIdentifiers, emptySet(), variableSets.leftOutputs);
                    ConjunctionStreamPlan rightPlan = buildSuffix(nextSuffix, variableSets.rightInputs, variableSets.rightOutputs);
                    return new CompoundStreamPlan(list(leftPlan, rightPlan), variableSets.identifiers, variableSets.extensions, requiredOutputs, new HashMap<>());
                }
            }

            public Pair<List<Resolvable<?>>, List<Resolvable<?>>> divide(List<Resolvable<?>> resolvables) {
                int splitAfter;
                Set<Variable.Retrievable> suffixVars = new HashSet<>(resolvables.get(resolvables.size() - 1).retrieves());
                for (splitAfter = resolvables.size() - 2; splitAfter > 0; splitAfter--) {
                    suffixVars.addAll(resolvables.get(splitAfter).retrieves());
                    Set<Variable.Retrievable> suffixBounds = intersection(boundsBefore.get(splitAfter), suffixVars);
                    Set<Variable.Retrievable> a = resolvables.get(splitAfter).retrieves();
                    Set<Variable.Retrievable> suffixFirstResolvableBounds = intersection(a, boundsBefore.get(splitAfter));
                    if (!suffixFirstResolvableBounds.equals(suffixBounds)) {
                        break;
                    }
                }
                assert splitAfter >= 0;
                return new Pair<>(resolvables.subList(0, splitAfter + 1), resolvables.subList(splitAfter + 1, resolvables.size()));
            }

            private ConjunctionStreamPlan flatten(ConjunctionStreamPlan plan) {
                if (plan.isResolvablePlan()) {
                    return plan;
                } else {
                    CompoundStreamPlan compoundPlan = plan.asCompoundStreamPlan();
                    assert compoundPlan.size() == 2;
                    List<ConjunctionStreamPlan> childPlans = new ArrayList<>();
                    for (int i = 0; i < 2; i++) {
                        ConjunctionStreamPlan child = compoundPlan.childAt(i);
                        if (child.isCompoundStreamPlan() && canFlattenInto(compoundPlan, child.asCompoundStreamPlan())) {
                            childPlans.addAll(flatten(child).asCompoundStreamPlan().childPlan);
                        } else {
                            childPlans.add(flatten(child));
                        }
                    }

                    Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild = new HashMap<>();
                    for (ConjunctionStreamPlan child : childPlans) {
                        boolean exclusivelyReads = child.isResolvablePlan() ||  // We don't re-use resolvable plans
                                isExclusiveReader(compoundPlan, child.asCompoundStreamPlan(), processorOutputs);
                        isExclusiveReaderOfChild.put(child, exclusivelyReads);
                    }

                    return new CompoundStreamPlan(childPlans, compoundPlan.identifiers(), compoundPlan.extensions(), compoundPlan.outputs(), isExclusiveReaderOfChild);
                }
            }

            private boolean canFlattenInto(CompoundStreamPlan parent, CompoundStreamPlan childToFlatten) {
                return isExclusiveReader(parent, childToFlatten, processorInputs) &&
                        boundsRemainSatisfied(parent, childToFlatten);
            }

            private static boolean isExclusiveReader(CompoundStreamPlan parent, CompoundStreamPlan child, Set<Variable.Retrievable> processorBounds) {
                return child.extensions().isEmpty() && child.identifierVariables.containsAll(difference(parent.identifierVariables, processorBounds));
            }

            private static boolean boundsRemainSatisfied(CompoundStreamPlan parent, CompoundStreamPlan childToFlatten) {
                return difference(
                        childToFlatten.childAt(1).identifierVariables,
                        union(parent.identifierVariables, childToFlatten.childAt(0).outputVariables))
                        .isEmpty() &&
                        union(
                                childToFlatten.asCompoundStreamPlan().childAt(1).outputs(),
                                childToFlatten.asCompoundStreamPlan().childAt(1).extensions())
                                .equals(childToFlatten.outputs());
            }

            private static class VariableSets {

                public final Set<Variable.Retrievable> identifiers;
                public final Set<Variable.Retrievable> extensions;
                public final Set<Variable.Retrievable> requiredOutputs;
                public final Set<Variable.Retrievable> leftIdentifiers;
                public final Set<Variable.Retrievable> leftOutputs;
                public final Set<Variable.Retrievable> rightInputs;
                public final Set<Variable.Retrievable> rightOutputs;

                private VariableSets(Set<Variable.Retrievable> identifiers, Set<Variable.Retrievable> extensions, Set<Variable.Retrievable> requiredOutputs,
                                     Set<Variable.Retrievable> leftIdentifiers, Set<Variable.Retrievable> leftOutputs,
                                     Set<Variable.Retrievable> rightInputs, Set<Variable.Retrievable> rightOutputs) {
                    this.identifiers = identifiers;
                    this.extensions = extensions;
                    this.requiredOutputs = requiredOutputs;
                    this.leftIdentifiers = leftIdentifiers;
                    this.leftOutputs = leftOutputs;
                    this.rightInputs = rightInputs;
                    this.rightOutputs = rightOutputs;
                }

                private static VariableSets create(List<Resolvable<?>> left, List<Resolvable<?>> right, Set<Variable.Retrievable> availableInputs, Set<Variable.Retrievable> requiredOutputs) {
                    Set<Variable.Retrievable> leftVariables = iterate(left).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
                    Set<Variable.Retrievable> rightVariables = iterate(right).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
                    Set<Variable.Retrievable> allUsedVariables = union(leftVariables, rightVariables);

                    Set<Variable.Retrievable> identifiers = intersection(availableInputs, allUsedVariables);
                    Set<Variable.Retrievable> extensions = difference(availableInputs, allUsedVariables);
                    Set<Variable.Retrievable> rightOutputs = difference(requiredOutputs, availableInputs);

                    Set<Variable.Retrievable> leftIdentifiers = intersection(identifiers, leftVariables);
                    Set<Variable.Retrievable> a = union(identifiers, leftVariables);
                    Set<Variable.Retrievable> b = union(rightVariables, rightOutputs);
                    Set<Variable.Retrievable> rightInputs = intersection(a, b);
                    Set<Variable.Retrievable> leftOutputs = difference(rightInputs, difference(identifiers, leftIdentifiers));

                    return new VariableSets(identifiers, extensions, requiredOutputs, leftIdentifiers, leftOutputs, rightInputs, rightOutputs);
                }

            }
        }

        public static Set<Variable.Retrievable> union(Set<Variable.Retrievable> a, Set<Variable.Retrievable> b) {
            Set<Variable.Retrievable> result = new HashSet<>(a);
            result.addAll(b);
            return result;
        }

        private static Set<Variable.Retrievable> difference(Set<Variable.Retrievable> a, Set<Variable.Retrievable> b) {
            Set<Variable.Retrievable> result = new HashSet<>(a);
            result.removeAll(b);
            return result;
        }
    }
}
