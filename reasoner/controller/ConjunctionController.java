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

import static com.vaticle.typedb.common.collection.Collections.set;
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
    final ConcurrentHashMap<Set<Variable.Retrievable>, List<ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>>> compoundStreamRegistries;
    final Set<Variable.Retrievable> outputVariables;

    ConjunctionController(Driver<CONTROLLER> driver, ResolvableConjunction conjunction, Set<Variable.Retrievable> outputVariables, Context context) {
        super(driver, context, () -> ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")");
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.retrievableControllers = new HashMap<>();
        this.concludableControllers = new HashMap<>();
        this.negationControllers = new HashMap<>();
        this.compoundStreamRegistries = new ConcurrentHashMap<>();
        this.outputVariables = outputVariables;
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

    List<Resolvable<?>> getPlan(Set<Variable.Retrievable> bounds) {
        Set<com.vaticle.typedb.core.pattern.variable.Variable> boundVariables = iterate(bounds).map(id -> conjunction.pattern().variable(id)).toSet();
        List<Resolvable<?>> plan = planner().getPlan(conjunction, boundVariables).plan();
        assert resolvables.size() == plan.size() && resolvables.containsAll(plan);
        return plan;
    }

    List<ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> getCompoundStreamRegistry(Set<Variable.Retrievable> retrievables) {
        return compoundStreamRegistries.computeIfAbsent(retrievables, rets -> {
            List<ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> registries = new ArrayList<>();
            getPlan(rets).forEach(r -> registries.add(new ConcurrentHashMap<>()));
            return registries;
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
        final List<Resolvable<?>> plan;
        final List<ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> compoundStreamRegistry;
        final Set<Variable.Retrievable> outputVariables;

        Processor(Driver<PROCESSOR> driver,
                  Driver<? extends ConjunctionController<?, PROCESSOR>> controller,
                  Context context, ConceptMap bounds, List<Resolvable<?>> plan,
                  Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.bounds = bounds;
            this.plan = plan;
            context.perfCounters().conjunctionProcessors.add(1);
            this.compoundStreamRegistry = controller.actor().getCompoundStreamRegistry(bounds.concepts().keySet());
            this.outputVariables = controller.actor().outputVariables;
        }

        public class CompoundStream extends TransformationStream<ConceptMap, ConceptMap> {

            private final Publisher<ConceptMap> leadingPublisher;
            private final List<Resolvable<?>> remainingPlan;
            private final Map<Publisher<ConceptMap>, ConceptMap> publisherPackets;
            private final ConceptMap initialPacket;
            private final AbstractProcessor<?, ?, ?, ?> processor;
            private final Set<Variable.Retrievable> remainingVariables;
            private final List<ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> remainingCompoundStreamRegistries;
            private final ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>> compoundStreamRegistry;


            CompoundStream(AbstractProcessor<?, ?, ?, ?> processor,
                           List<Resolvable<?>> plan, List<ConcurrentHashMap<ConceptMap, PoolingStream.BufferedFanStream<ConceptMap>>> compoundStreamRegistriesForPlan,
                           ConceptMap initialPacket) {
                super(processor, new SubscriberRegistry.Multi<>(), new PublisherRegistry.Multi<>());
                this.processor = processor;
                assert plan.size() > 0;
                this.initialPacket = initialPacket;
                this.remainingPlan = new ArrayList<>(plan);
                this.remainingCompoundStreamRegistries = new ArrayList<>(compoundStreamRegistriesForPlan);
                this.compoundStreamRegistry = this.remainingCompoundStreamRegistries.remove(0);
                this.publisherPackets = new HashMap<>();
                this.leadingPublisher = nextCompoundLeader(this.remainingPlan.remove(0), initialPacket);
                this.leadingPublisher.registerSubscriber(this);
                processor().context().perfCounters().compoundStreams.add(1);
                this.remainingVariables = iterate(this.remainingPlan)
                        .flatMap(resolvable -> iterate(resolvable.retrieves()))
                        .toSet();
            }

            @Override
            public Either<Publisher<ConceptMap>, Set<ConceptMap>> accept(Publisher<ConceptMap> publisher,
                                                                         ConceptMap packet) {
                ConceptMap mergedPacket = merge(initialPacket, packet);
                if (leadingPublisher.equals(publisher)) {
                    if (remainingPlan.size() == 0) {  // For a single item plan
                        return Either.second(set(mergedPacket.filter(outputVariables)));
                    } else {
                        Publisher<ConceptMap> follower;  // TODO: Creation of a new publisher should be delegated to the owner of this operation
                        if (remainingPlan.size() == 1) {
                            follower = mergeWithRemainingVars(nextCompoundLeader(remainingPlan.get(0), mergedPacket), mergedPacket);
                        } else {
                            follower = mergeWithRemainingVars(getOrCreateCompoundStream(mergedPacket), mergedPacket);
                        }
                        publisherPackets.put(follower, mergedPacket);
                        return Either.first(follower);
                    }
                } else {

//                    ConceptMap compoundedPacket = merge(mergedPacket, publisherPackets.get(publisher));
//                    assert compoundedPacket.equals(merge(publisherPackets.get(publisher), mergedPacket));
//                    return Either.second(set(compoundedPacket));
                    // TODO: This is a bad hack for testing. We need to fix it for outputVariables to include the ones in the remaining plan
                    return Either.second(set(mergedPacket.filter(outputVariables)));
                }
            }

            private Publisher<ConceptMap> getOrCreateCompoundStream(ConceptMap mergedPacket) {
                ConceptMap filteredPacket = mergedPacket.filter(remainingVariables);
                return compoundStreamRegistry.computeIfAbsent(filteredPacket, packet -> {
                    CompoundStream compoundStream = new CompoundStream(processor, remainingPlan, remainingCompoundStreamRegistries, filteredPacket);
                    PoolingStream.BufferedFanStream<ConceptMap> bufferedStream = PoolingStream.BufferedFanStream.fanOut(compoundStream.processor);
                    compoundStream.registerSubscriber(bufferedStream);
                    return bufferedStream;
                });
            }

            private Publisher<ConceptMap> mergeWithRemainingVars(Publisher<ConceptMap> s, ConceptMap mergedPacket) {
                // TODO: You want to create a transformation stream which cartesian-products the variables which aren't passed down.
                // This prevents the multi-subscriber problem and (hopefully) gives you the right result
                Set<Variable.Retrievable> joinVars = iterate(mergedPacket.concepts().keySet()).filter(v -> !remainingVariables.contains(v) && outputVariables.contains(v)).toSet();
                ConceptMap remainingBounds = mergedPacket.filter(joinVars);
                return remainingBounds.concepts().isEmpty() ? s : s.map(conceptMap -> merge(conceptMap, remainingBounds));
            }
        }

        private InputPort<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            InputPort<ConceptMap> input = createInputPort();
            if (planElement.isRetrievable()) {
                requestConnection(new RetrievableRequest(input.identifier(), driver(), planElement.asRetrievable(),
                        carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isConcludable()) {
                requestConnection(new ConcludableRequest(input.identifier(), driver(), planElement.asConcludable(),
                        carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isNegated()) {
                requestConnection(new NegatedRequest(input.identifier(), driver(), planElement.asNegated(),
                        carriedBounds.filter(planElement.retrieves())));
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return input;
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
