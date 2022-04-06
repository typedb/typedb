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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.answer.Mapping;
import com.vaticle.typedb.core.reasoner.computation.actor.Connector;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.Input;
import com.vaticle.typedb.core.reasoner.controller.Registry.ResolverView;
import com.vaticle.typedb.core.reasoner.utils.Planner;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ConjunctionController<OUTPUT,
        CONTROLLER extends ConjunctionController<OUTPUT, CONTROLLER, PROCESSOR>,
        PROCESSOR extends Processor<ConceptMap, OUTPUT, ?, PROCESSOR>>
        extends Controller<ConceptMap, ConceptMap, OUTPUT, ConjunctionController.FromConjunctionRequest<?>, PROCESSOR, CONTROLLER> {

    protected final Conjunction conjunction;
    private final Set<Resolvable<?>> resolvables;
    private final Set<Negated> negateds;
    private final Map<Retrievable, ResolverView.FilteredRetrievable> retrievableControllers;
    private final Map<Concludable, ResolverView.MappedConcludable> concludableControllers;
    private final Map<Negated, ResolverView.FilteredNegation> negationControllers;
    private List<Resolvable<?>> plan;

    public ConjunctionController(Driver<CONTROLLER> driver, Conjunction conjunction,
                                 ActorExecutorGroup executorService, Registry registry) {
        super(driver, executorService, registry,
              () -> ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")");
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
            concludableControllers.put(c, registry().registerConcludableController(c));
        });
        iterate(retrievables).forEachRemaining(r -> {
            retrievableControllers.put(r, registry().registerRetrievableController(r));
        });
        iterate(conjunction.negations()).forEachRemaining(negation -> {
            Negated negated = new Negated(negation);
            try {
                negationControllers.put(negated, registry().registerNegationController(negated, conjunction));
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

    protected static ConceptMap merge(ConceptMap c1, ConceptMap c2) {
        Map<Variable.Retrievable, Concept> compounded = new HashMap<>(c1.concepts());
        compounded.putAll(c2.concepts());
        return new ConceptMap(compounded);
    }

    @Override
    protected void resolveController(FromConjunctionRequest<?> connectionRequest) {
        if (isTerminated()) return;
        if (connectionRequest.isRetrievable()) {
            ConjunctionProcessor.RetrievableRequest req = connectionRequest.asRetrievable();
            ResolverView.FilteredRetrievable controllerView = retrievableControllers.get(req.controllerId());
            ConceptMap newPID = req.bounds().filter(controllerView.filter());
            Connector<ConceptMap, ConceptMap> connector = new Connector<>(req.inputId(), req.bounds())
                    .withMap(c -> merge(c, req.bounds()))
                    .withNewBounds(newPID);
            controllerView.controller().execute(actor -> actor.resolveProcessor(connector));
        } else if (connectionRequest.isConcludable()) {
            ConjunctionProcessor.ConcludableRequest req = connectionRequest.asConcludable();
            ResolverView.MappedConcludable controllerView = concludableControllers.get(req.controllerId());
            Mapping mapping = Mapping.of(controllerView.mapping());
            ConceptMap newPID = mapping.transform(req.bounds());
            Connector<ConceptMap, ConceptMap> connector = new Connector<>(req.inputId(), req.bounds())
                    .withMap(mapping::unTransform)
                    .withNewBounds(newPID);
            controllerView.controller().execute(actor -> actor.resolveProcessor(connector));
        } else if (connectionRequest.isNegated()) {
            ConjunctionProcessor.NegatedRequest req = connectionRequest.asNegated();
            ResolverView.FilteredNegation controllerView = negationControllers.get(req.controllerId());
            ConceptMap newPID = req.bounds().filter(controllerView.filter());
            Connector<ConceptMap, ConceptMap> connector = new Connector<>(req.inputId(), req.bounds())
                    .withMap(c -> merge(c, req.bounds()))
                    .withNewBounds(newPID);
            controllerView.controller().execute(actor -> actor.resolveProcessor(connector));
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    public static class FromConjunctionRequest<CONTROLLER_ID> extends Connector.Request<CONTROLLER_ID, ConceptMap, ConceptMap> {
        protected FromConjunctionRequest(Reactive.Identifier<ConceptMap, ?> inputId, CONTROLLER_ID controller_id,
                                         ConceptMap conceptMap) {
            super(inputId, controller_id, conceptMap);
        }

        public boolean isRetrievable() {
            return false;
        }

        public ConjunctionProcessor.RetrievableRequest asRetrievable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isConcludable() {
            return false;
        }

        public ConjunctionProcessor.ConcludableRequest asConcludable() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isNegated() {
            return false;
        }

        public ConjunctionProcessor.NegatedRequest asNegated() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

    }

    protected static abstract class ConjunctionProcessor<OUTPUT, PROCESSOR extends ConjunctionProcessor<OUTPUT, PROCESSOR>>
            extends Processor<ConceptMap, OUTPUT, FromConjunctionRequest<?>, PROCESSOR> {
        protected final ConceptMap bounds;
        protected final List<Resolvable<?>> plan;

        protected ConjunctionProcessor(Driver<PROCESSOR> driver,
                                       Driver<? extends ConjunctionController<OUTPUT, ?, PROCESSOR>> controller,
                                       Driver<Monitor> monitor, ConceptMap bounds, List<Resolvable<?>> plan,
                                       Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.bounds = bounds;
            this.plan = plan;
        }

        protected Input<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            // TODO: Rethink this ugly structure for compound reactives
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

        public static class RetrievableRequest extends FromConjunctionRequest<Retrievable> {

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

        static class ConcludableRequest extends FromConjunctionRequest<Concludable> {

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

        // TODO: Negated or Negation request?
        static class NegatedRequest extends FromConjunctionRequest<Negated> {

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
