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

package com.vaticle.typedb.core.reasoner.controller;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.answer.Mapping;
import com.vaticle.typedb.core.reasoner.common.Planner;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.Processor.ConcludableRequest;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.Processor.NegatedRequest;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.Processor.RetrievableRequest;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView.FilteredNegation;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView.FilteredRetrievable;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView.MappedConcludable;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.InputPort;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.processor.reactive.TransformationStream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.Operator;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.controller.ConcludableController.Processor.Match.withExplainable;

public abstract class ConjunctionController<
        OUTPUT, CONTROLLER extends ConjunctionController<OUTPUT, CONTROLLER, PROCESSOR>,
        PROCESSOR extends AbstractProcessor<ConceptMap, OUTPUT, ?, PROCESSOR>
        > extends AbstractController<
        ConceptMap, ConceptMap, OUTPUT, ConjunctionController.Request<?, ?>, PROCESSOR, CONTROLLER
        > {

    protected final Conjunction conjunction;
    private final Set<Resolvable<?>> resolvables;
    private final Set<Negated> negateds;
    private final Map<Retrievable, FilteredRetrievable> retrievableControllers;
    private final Map<Concludable, MappedConcludable> concludableControllers;
    private final Map<Negated, FilteredNegation> negationControllers;
    private List<Resolvable<?>> plan;

    public ConjunctionController(Driver<CONTROLLER> driver, Conjunction conjunction, Context context) {
        super(driver, context, () -> ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")");
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.negateds = new HashSet<>();
        this.retrievableControllers = new HashMap<>();
        this.concludableControllers = new HashMap<>();
        this.negationControllers = new HashMap<>();
    }

    @Override
    public void setUpUpstreamControllers() {
        assert resolvables.isEmpty();
        Set<Concludable> concludables = concludablesTriggeringRules();
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludables);
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);
        iterate(concludables).forEachRemaining(c -> {
            concludableControllers.put(c, registry().getOrCreateConcludable(c));
        });
        iterate(retrievables).forEachRemaining(r -> {
            retrievableControllers.put(r, registry().createRetrievable(r));
        });
        iterate(conjunction.negations()).forEachRemaining(negation -> {
            Negated negated = new Negated(negation);
            try {
                negationControllers.put(negated, registry().createNegation(negated, conjunction));
                negateds.add(negated);
            } catch (TypeDBException e) {
                terminate(e);
            }
        });
    }

    abstract Set<Concludable> concludablesTriggeringRules();

    protected List<Resolvable<?>> plan() {
        if (plan == null) {
            plan = Planner.plan(resolvables, new HashMap<>(), set());
            plan.addAll(negateds);
        }
        return plan;
    }

    protected static ConceptMap merge(ConceptMap into, ConceptMap from) {
        Map<Variable.Retrievable, Concept> compounded = new HashMap<>(into.concepts());
        compounded.putAll(from.concepts());
        return new ConceptMap(compounded, into.explainables().merge(from.explainables()));
    }

    @Override
    public void routeConnectionRequest(Request<?, ?> connectionRequest) {
        if (isTerminated()) return;
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

    public static class Request<
            CONTROLLER_ID, CONTROLLER extends AbstractController<ConceptMap, ?, ConceptMap, ?, ?, ?>
            > extends AbstractRequest<CONTROLLER_ID, ConceptMap, ConceptMap, CONTROLLER> {
        protected Request(Reactive.Identifier<ConceptMap, ?> inputPortId, CONTROLLER_ID controller_id,
                          ConceptMap conceptMap) {
            super(inputPortId, controller_id, conceptMap);
        }

        public boolean isRetrievable() {
            return false;
        }

        public RetrievableRequest asRetrievable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isConcludable() {
            return false;
        }

        public ConcludableRequest asConcludable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isNegated() {
            return false;
        }

        public NegatedRequest asNegated() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

    }

    protected abstract static class Processor<OUTPUT, PROCESSOR extends Processor<OUTPUT, PROCESSOR>>
            extends AbstractProcessor<ConceptMap, OUTPUT, Request<?, ?>, PROCESSOR> {

        protected final ConceptMap bounds;
        protected final List<Resolvable<?>> plan;

        protected Processor(Driver<PROCESSOR> driver,
                                Driver<? extends ConjunctionController<OUTPUT, ?, PROCESSOR>> controller,
                                Context context, ConceptMap bounds, List<Resolvable<?>> plan,
                                Supplier<String> debugName) {
            super(driver, controller, context, debugName);
            this.bounds = bounds;
            this.plan = plan;
        }

        public class CompoundOperator implements Operator.Transformer<ConceptMap, ConceptMap> {

            private final Publisher<ConceptMap> leadingPublisher;
            private final List<Resolvable<?>> remainingPlan;
            private final Map<Publisher<ConceptMap>, ConceptMap> publisherPackets;
            private final ConceptMap initialPacket;
            private final AbstractProcessor<?, ?, ?, ?> processor;

            public CompoundOperator(AbstractProcessor<?, ?, ?, ?> processor, List<Resolvable<?>> plan,
                                    ConceptMap initialPacket) {
                this.processor = processor;
                assert plan.size() > 0;
                this.initialPacket = initialPacket;
                this.remainingPlan = new ArrayList<>(plan);
                this.publisherPackets = new HashMap<>();
                this.leadingPublisher = nextCompoundLeader(this.remainingPlan.remove(0), initialPacket);
            }

            @Override
            public Set<Publisher<ConceptMap>> initialNewPublishers() {
                return set(this.leadingPublisher);
            }

            @Override
            public Either<Publisher<ConceptMap>, Set<ConceptMap>> accept(Publisher<ConceptMap> publisher,
                                                                         ConceptMap packet) {
                ConceptMap mergedPacket = merge(initialPacket, packet);
                if (leadingPublisher.equals(publisher)) {
                    if (remainingPlan.size() == 0) {  // For a single item plan
                        return Either.second(set(mergedPacket));
                    } else {
                        Publisher<ConceptMap> follower;  // TODO: Creation of a new publisher should be delegated to the owner of this operation
                        if (remainingPlan.size() == 1) {
                            follower = nextCompoundLeader(remainingPlan.get(0), mergedPacket);
                        } else {
                            follower = TransformationStream.fanIn(
                                    processor,
                                    new CompoundOperator(processor, remainingPlan, mergedPacket)
                            ).buffer();
                        }
                        publisherPackets.put(follower, mergedPacket);
                        return Either.first(follower);
                    }
                } else {
                    ConceptMap compoundedPacket = merge(mergedPacket, publisherPackets.get(publisher));
                    assert compoundedPacket.equals(merge(publisherPackets.get(publisher), mergedPacket));
                    return Either.second(set(compoundedPacket));
                }
            }

        }

        protected InputPort<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            InputPort<ConceptMap> input = createInputPort();
            if (planElement.isRetrievable()) {
                requestConnection(new RetrievableRequest(input.identifier(), planElement.asRetrievable(),
                                                         carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isConcludable()) {
                requestConnection(new ConcludableRequest(input.identifier(), planElement.asConcludable(),
                                                         carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isNegated()) {
                requestConnection(new NegatedRequest(input.identifier(), planElement.asNegated(),
                                                     carriedBounds.filter(planElement.retrieves())));
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return input;
        }

        public static class RetrievableRequest extends Request<Retrievable, RetrievableController> {

            public RetrievableRequest(Reactive.Identifier<ConceptMap, ?> inputPortId, Retrievable controllerId,
                                      ConceptMap processorId) {
                super(inputPortId, controllerId, processorId);
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

        static class ConcludableRequest extends Request<Concludable, ConcludableController.Match> {

            public ConcludableRequest(Reactive.Identifier<ConceptMap, ?> inputPortId, Concludable controllerId,
                                      ConceptMap processorId) {
                super(inputPortId, controllerId, processorId);
            }

            public boolean isConcludable() {
                return true;
            }

            public ConcludableRequest asConcludable() {
                return this;
            }

        }

        static class NegatedRequest extends Request<Negated, NegationController> {

            protected NegatedRequest(Reactive.Identifier<ConceptMap, ?> inputPortId, Negated controllerId,
                                     ConceptMap processorId) {
                super(inputPortId, controllerId, processorId);
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
