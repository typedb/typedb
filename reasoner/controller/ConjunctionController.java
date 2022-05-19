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
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry.ControllerView;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.Connector;
import com.vaticle.typedb.core.reasoner.processor.Connector.AbstractRequest;
import com.vaticle.typedb.core.reasoner.processor.Input;
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

public abstract class ConjunctionController<OUTPUT,
        CONTROLLER extends ConjunctionController<OUTPUT, CONTROLLER, REACTIVE_BLOCK>,
        REACTIVE_BLOCK extends AbstractProcessor<ConceptMap, OUTPUT, ?, REACTIVE_BLOCK>
        > extends AbstractController<
        ConceptMap,
        ConceptMap,
        OUTPUT,
        ConjunctionController.Request<?>,
        REACTIVE_BLOCK,
        CONTROLLER
        > {

    protected final Conjunction conjunction;
    private final Set<Resolvable<?>> resolvables;
    private final Set<Negated> negateds;
    private final Map<Retrievable, ControllerView.FilteredRetrievable> retrievableControllers;
    private final Map<Concludable, ControllerView.MappedConcludable> concludableControllers;
    private final Map<Negated, ControllerView.FilteredNegation> negationControllers;
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
    public void routeConnectionRequest(Request<?> connectionRequest) {
        if (isTerminated()) return;
        if (connectionRequest.isRetrievable()) {
            Processor.RetrievableRequest req = connectionRequest.asRetrievable();
            ControllerRegistry.ControllerView.FilteredRetrievable controllerView = retrievableControllers.get(req.controllerId());
            ConceptMap newPID = req.bounds().filter(controllerView.filter());
            Connector<ConceptMap, ConceptMap> connector = new Connector<>(req.inputId(), req.bounds())
                    .withMap(c -> merge(c, req.bounds()))
                    .withNewBounds(newPID);
            controllerView.controller().execute(actor -> actor.establishProcessorConnection(connector));
        } else if (connectionRequest.isConcludable()) {
            Processor.ConcludableRequest req = connectionRequest.asConcludable();
            ControllerView.MappedConcludable controllerView = concludableControllers.get(req.controllerId());
            Mapping mapping = Mapping.of(controllerView.mapping());
            ConceptMap newPID = mapping.transform(req.bounds());
            Connector<ConceptMap, ConceptMap> connector = new Connector<>(req.inputId(), req.bounds())
                    .withMap(mapping::unTransform)
                    .withMap(c -> remapExplainable(c, req.controllerId()))
                    .withNewBounds(newPID);
            controllerView.controller().execute(actor -> actor.establishProcessorConnection(connector));
        } else if (connectionRequest.isNegated()) {
            Processor.NegatedRequest req = connectionRequest.asNegated();
            ControllerRegistry.ControllerView.FilteredNegation controllerView = negationControllers.get(req.controllerId());
            ConceptMap newPID = req.bounds().filter(controllerView.filter());
            Connector<ConceptMap, ConceptMap> connector = new Connector<>(req.inputId(), req.bounds())
                    .withMap(c -> merge(c, req.bounds()))
                    .withNewBounds(newPID);
            controllerView.controller().execute(actor -> actor.establishProcessorConnection(connector));
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

    public static class Request<CONTROLLER_ID> extends AbstractRequest<CONTROLLER_ID, ConceptMap, ConceptMap> {
        protected Request(Reactive.Identifier<ConceptMap, ?> inputId, CONTROLLER_ID controller_id,
                          ConceptMap conceptMap) {
            super(inputId, controller_id, conceptMap);
        }

        public boolean isRetrievable() {
            return false;
        }

        public Processor.RetrievableRequest asRetrievable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isConcludable() {
            return false;
        }

        public Processor.ConcludableRequest asConcludable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isNegated() {
            return false;
        }

        public Processor.NegatedRequest asNegated() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

    }

    protected abstract static class Processor<OUTPUT, REACTIVE_BLOCK extends Processor<OUTPUT, REACTIVE_BLOCK>>
            extends AbstractProcessor<ConceptMap, OUTPUT, Request<?>, REACTIVE_BLOCK> {

        protected final ConceptMap bounds;
        protected final List<Resolvable<?>> plan;

        protected Processor(Driver<REACTIVE_BLOCK> driver,
                                Driver<? extends ConjunctionController<OUTPUT, ?, REACTIVE_BLOCK>> controller,
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

        protected Input<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            Input<ConceptMap> input = createInput();
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

        public static class RetrievableRequest extends Request<Retrievable> {

            public RetrievableRequest(Reactive.Identifier<ConceptMap, ?> inputId, Retrievable controllerId,
                                      ConceptMap processorId) {
                super(inputId, controllerId, processorId);
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

            public ConcludableRequest(Reactive.Identifier<ConceptMap, ?> inputId, Concludable controllerId,
                                      ConceptMap processorId) {
                super(inputId, controllerId, processorId);
            }

            public boolean isConcludable() {
                return true;
            }

            public ConcludableRequest asConcludable() {
                return this;
            }

        }

        static class NegatedRequest extends Request<Negated> {

            protected NegatedRequest(Reactive.Identifier<ConceptMap, ?> inputId, Negated controllerId,
                                     ConceptMap processorId) {
                super(inputId, controllerId, processorId);
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
