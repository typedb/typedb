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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
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
import com.vaticle.typedb.core.reasoner.planner.ConjunctionStreamPlan;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.PoolingStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
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

    final Map<Set<Variable.Retrievable>, PerfCounters.Counter> debug__compoundStreamCounters;

    ConjunctionController(Driver<CONTROLLER> driver, ResolvableConjunction conjunction, Set<Variable.Retrievable> outputVariables, Context context) {
        super(driver, context, () -> ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")");
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.retrievableControllers = new HashMap<>();
        this.concludableControllers = new HashMap<>();
        this.negationControllers = new HashMap<>();
        this.outputVariables = outputVariables;
        this.plans = new ConcurrentHashMap<>();

        assert conjunction.pattern().retrieves().containsAll(outputVariables);
        debug__compoundStreamCounters = new HashMap<>();
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
            debug__compoundStreamCounters.put(inputBounds, processorContext().perfCounters().register(conjunction + "::" + String.join(",", iterate(inputBounds).map(b -> b.toString()).toList())));
            Set<com.vaticle.typedb.core.pattern.variable.Variable> boundVariables = iterate(inputBounds).map(id -> conjunction.pattern().variable(id)).toSet();
            List<Resolvable<?>> plan = planner().getPlan(conjunction, boundVariables).plan();
            assert resolvables.size() == plan.size() && resolvables.containsAll(plan);
            return ConjunctionStreamPlan.createFlattenedConjunctionStreamPlan(plan, inputBounds, outputVariables);
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
            private final ConjunctionStreamPlan plan;
            private final ConceptMap identifierBounds;
            private final AbstractProcessor<?, ?, ?, ?> processor;
            private final Map<Publisher<ConceptMap>, Integer> whichChild;


            CompoundStream(AbstractProcessor<?, ?, ?, ?> processor, ConjunctionStreamPlan plan, ConceptMap identifierBounds) {
                super(processor, new SubscriberRegistry.Multi<>(), new PublisherRegistry.Multi<>());
                this.processor = processor;
                this.identifierBounds = identifierBounds;
                this.plan = plan;
                Publisher<ConceptMap> leftChild;
                if (plan.isCompoundStream()) {
                    leftChild = spawnPlanElement(plan.asCompoundStreamPlan().ithChild(0), identifierBounds);
                } else {
                    leftChild = spawnPlanElement(plan.asResolvablePlan(), identifierBounds);
                }
                leftChild.registerSubscriber(this);
                this.whichChild = new HashMap<>();
                this.whichChild.put(leftChild, 0);
                processor().context().perfCounters().compoundStreams.add(1);
                ((PerfCounters.Counter)((ConjunctionController)(processor.controller.actor())).debug__compoundStreamCounters.get(new HashSet<>(((ConjunctionController.Processor)processor).bounds.concepts().keySet()))).add(1);
            }

            private Publisher<ConceptMap> spawnPlanElement(ConjunctionStreamPlan planElement, ConceptMap bounds) {
                ConceptMap extension = filterOutputsWithExplainables(bounds, planElement.extendOutputWithVariables());
                ConceptMap identifierBounds = bounds.filter(planElement.identifierVariables());
                assert planElement.identifierVariables().size() == identifierBounds.concepts().size() &&  identifierBounds.concepts().keySet().containsAll(planElement.identifierVariables());
                Publisher<ConceptMap> publisher;
                if (planElement.isResolvable()) {
                    publisher = spawnResolvableElement(planElement.asResolvablePlan(), identifierBounds);
                } else if (planElement.isCompoundStream()) {
                    publisher = spawnCompoundStream(planElement.asCompoundStreamPlan(), identifierBounds);
                } else throw TypeDBException.of(ILLEGAL_STATE);

                // TODO: Filter results to output? and then extend?
                return extendWithBounds(publisher, extension);
            }

            @Override
            public Either<Publisher<ConceptMap>, Set<ConceptMap>> accept(Publisher<ConceptMap> publisher,
                                                                         ConceptMap packet) {
                context().perfCounters().compoundStreamAccepts.add(1);
                ConceptMap mergedPacket = merge(identifierBounds, packet);
                int nextChild = whichChild.get(publisher) + 1;

                // assert on packet. NOT mergedPacket.
                assert plan.isResolvable() || (
                        ConjunctionStreamPlan.union(plan.asCompoundStreamPlan().ithChild(nextChild-1).outputVariables(), plan.asCompoundStreamPlan().ithChild(nextChild-1).extendOutputWithVariables()).size() == packet.concepts().size() &&
                                packet.concepts().keySet().containsAll(ConjunctionStreamPlan.union(plan.asCompoundStreamPlan().ithChild(nextChild-1).outputVariables(),
                                plan.asCompoundStreamPlan().ithChild(nextChild-1).extendOutputWithVariables())));
                if (plan.isCompoundStream() && nextChild < plan.asCompoundStreamPlan().size()) {
                    Publisher<ConceptMap> follower = spawnPlanElement(plan.asCompoundStreamPlan().ithChild(nextChild), mergedPacket);
                    whichChild.put(follower, nextChild);
                    return Either.first(follower);
                } else {
                    return Either.second(set(filterOutputsWithExplainables(mergedPacket, plan.outputVariables())));
                }
            }

            private Publisher<ConceptMap> spawnCompoundStream(ConjunctionStreamPlan.CompoundStreamPlan planElement, ConceptMap mergedPacket) {
                ConceptMap identifyingBounds = mergedPacket.filter(planElement.identifierVariables());
                return compoundStreamRegistry.computeIfAbsent(planElement, _x -> new HashMap<>()).computeIfAbsent(identifyingBounds, packet -> {
                    CompoundStream compoundStream = new CompoundStream(processor, planElement, identifyingBounds);
                    PoolingStream.BufferedFanStream<ConceptMap> bufferedStream = PoolingStream.BufferedFanStream.fanOut(compoundStream.processor);
                    // TODO: Check if this is true - you only need to buffer if extendWithOutput is non-empty. (Because then we won't have multiple readers?)
                    // TODO: Should I create a local copy of planElement.outputVariables so it's not a local creeping into the scope?
                    //          Plan element will live as long as the processor either way
                    compoundStream.map(conceptMap -> filterOutputsWithExplainables(conceptMap, planElement.outputVariables())).registerSubscriber(bufferedStream);
                    return bufferedStream;
                });
            }

            private Reactive.Publisher<ConceptMap> spawnResolvableElement(ConjunctionStreamPlan.ResolvablePlan resolvablePlan, ConceptMap carriedBounds) {
                InputPort<ConceptMap> input = createInputPort();
                Resolvable<?> resolvable = resolvablePlan.resolvable();
                if (resolvable.isRetrievable()) {
                    requestConnection(new RetrievableRequest(input.identifier(), driver(), resolvable.asRetrievable(),
                            carriedBounds.filter(resolvable.retrieves())));
                } else if (resolvable.isConcludable()) {
                    requestConnection(new ConcludableRequest(input.identifier(), driver(), resolvable.asConcludable(),
                            carriedBounds.filter(resolvable.retrieves())));
                } else if (resolvable.isNegated()) {
                    requestConnection(new NegatedRequest(input.identifier(), driver(), resolvable.asNegated(),
                            carriedBounds.filter(resolvable.retrieves())));
                } else {
                    throw TypeDBException.of(ILLEGAL_STATE);
                }
                return input.map(conceptMap -> filterOutputsWithExplainables(conceptMap, resolvablePlan.outputVariables()));
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


}
