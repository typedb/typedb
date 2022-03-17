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
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Monitor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.NoOpStream;
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
        extends Controller<ConceptMap, ConceptMap, OUTPUT, PROCESSOR, CONTROLLER> {

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
    public void setUpUpstreamProviders() {
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

    protected ResolverView.FilteredRetrievable retrievableProvider(Retrievable retrievable) {
        return retrievableControllers.get(retrievable);
    }

    protected ResolverView.MappedConcludable concludableProvider(Concludable concludable) {
        return concludableControllers.get(concludable);
    }

    protected ResolverView.FilteredNegation negationProvider(Negated negated) {
        return negationControllers.get(negated);
    }

    protected static ConceptMap merge(ConceptMap c1, ConceptMap c2) {
        Map<Variable.Retrievable, Concept> compounded = new HashMap<>(c1.concepts());
        compounded.putAll(c2.concepts());
        return new ConceptMap(compounded);
    }

    static class RetrievableRequest<P extends Processor<ConceptMap, ?, ?, P>, C extends ConjunctionController<?, C, P>>
            extends ProviderRequest<Retrievable, ConceptMap, RetrievableController, ConceptMap, P, C, RetrievableRequest<P, C>> {

        public RetrievableRequest(Driver<P> recProcessor, long recEndpointId, Retrievable provControllerId,
                                  ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, ConceptMap, RetrievableRequest<P, C>, P, ?> getConnectionBuilder(C controller) {
            ResolverView.FilteredRetrievable controllerView = controller.retrievableProvider(providingControllerId());
            ConceptMap newPID = providingProcessorId().filter(controllerView.filter());
            return new Connection.Builder<>(controllerView.controller(), this)
                    .withMap(c -> merge(c, providingProcessorId()))
                    .withNewProcessorId(newPID);
        }
    }

    static class ConcludableRequest<P extends Processor<ConceptMap, ?, ?, P>, C extends ConjunctionController<?, C, P>>
            extends ProviderRequest<Concludable, ConceptMap, ConcludableController, ConceptMap, P, C, ConcludableRequest<P, C>> {

        public ConcludableRequest(Driver<P> recProcessor, long recEndpointId, Concludable provControllerId,
                                  ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, ConceptMap, ConcludableRequest<P, C>, P, ?> getConnectionBuilder(C controller) {
            ResolverView.MappedConcludable controllerView = controller.concludableProvider(providingControllerId());
            Mapping mapping = Mapping.of(controllerView.mapping());
            ConceptMap newPID = mapping.transform(providingProcessorId());
            return new Connection.Builder<>(controllerView.controller(), this)
                    .withMap(mapping::unTransform)
                    .withNewProcessorId(newPID);
        }
    }

    // TODO: Negated request or Negation?
    static class NegatedRequest<P extends Processor<ConceptMap, ?, ?, P>, C extends ConjunctionController<?, C, P>>
            extends ProviderRequest<Negated, ConceptMap, NegationController, ConceptMap, P, C, NegatedRequest<P, C>> {

        protected NegatedRequest(Driver<P> recProcessor, long recEndpointId, Negated provControllerId,
                                 ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, ConceptMap, NegatedRequest<P, C>, P, ?> getConnectionBuilder(C controller) {
            ResolverView.FilteredNegation controllerView = controller.negationProvider(providingControllerId());
            ConceptMap newPID = providingProcessorId().filter(controllerView.filter());
            return new Connection.Builder<>(controllerView.controller(), this)
                    .withMap(c -> merge(c, providingProcessorId()))
                    .withNewProcessorId(newPID);
        }
    }

    protected static abstract class ConjunctionProcessor<OUTPUT,
            CONTROLLER extends ConjunctionController<OUTPUT, CONTROLLER, PROCESSOR>,
            PROCESSOR extends Processor<ConceptMap, OUTPUT, CONTROLLER, PROCESSOR>>
            extends Processor<ConceptMap, OUTPUT, CONTROLLER, PROCESSOR> {
        protected final ConceptMap bounds;
        protected final List<Resolvable<?>> plan;

        protected ConjunctionProcessor(Driver<PROCESSOR> driver, Driver<CONTROLLER> controller,
                                       Driver<Monitor> monitor, ConceptMap bounds, List<Resolvable<?>> plan,
                                       Supplier<String> debugName) {
            super(driver, controller, monitor, debugName);
            this.bounds = bounds;
            this.plan = plan;
        }

        @Override
        public void setUp() {
            setOutlet(new NoOpStream<>(this));
        }

        protected InletEndpoint<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            // TODO: Rethink this ugly structure for compound reactives
            InletEndpoint<ConceptMap> endpoint = createReceivingEndpoint();
            if (planElement.isRetrievable()) {
                requestProvider(new RetrievableRequest<>(driver(), endpoint.id(), planElement.asRetrievable(),
                                                         carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isConcludable()) {
                requestProvider(new ConcludableRequest<>(driver(), endpoint.id(), planElement.asConcludable(),
                                                         carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isNegated()) {
                requestProvider(new NegatedRequest<>(driver(), endpoint.id(), planElement.asNegated(),
                                                     carriedBounds.filter(planElement.retrieves())));
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return endpoint;
        }

    }

}
