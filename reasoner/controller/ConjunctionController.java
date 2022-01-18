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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor.Request;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry.ResolverView;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;

public abstract class ConjunctionController<OUTPUT,
        CONTROLLER extends ConjunctionController<OUTPUT, CONTROLLER, PROCESSOR>,
        PROCESSOR extends Processor<ConceptMap, OUTPUT, ?, PROCESSOR>>
        extends Controller<ConceptMap, ConceptMap, OUTPUT, PROCESSOR, CONTROLLER> {

    protected final Conjunction conjunction;
    private final Set<Resolvable<?>> resolvables;
    private final Set<Negated> negateds;
    private final List<Pair<Retrievable, ResolverView.FilteredRetrievable>> retrievableControllers;
    private final Map<Concludable, ResolverView.MappedConcludable> concludableControllers;

    public ConjunctionController(Driver<CONTROLLER> driver, Conjunction conjunction,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")", executorService,
              registry);
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.negateds = new HashSet<>();
        this.retrievableControllers = new ArrayList<>();
        this.concludableControllers = new HashMap<>();
    }

    @Override
    public void setUpUpstreamProviders() {
        Set<Concludable> concludables = concludablesTriggeringRules();
        iterate(concludables).forEachRemaining(c -> {
            concludableControllers.put(c, registry().registerConcludableController(c));
        });
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludables);
        iterate(retrievables).forEachRemaining(r -> {
            retrievableControllers.add(new Pair<>(r, registry().registerRetrievableController(r)));
        });
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);
    }

    abstract Set<Concludable> concludablesTriggeringRules();

    protected List<Resolvable<?>> plan() {
        return new ArrayList<>(resolvables);
    }

    protected ResolverView.FilteredRetrievable retrievableProvider(Retrievable retrievable) {
        Optional<ResolverView.FilteredRetrievable> controller =
                iterate(retrievableControllers).filter(r -> r.first() == retrievable).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected ResolverView.MappedConcludable concludableProvider(Concludable concludable) {
        return concludableControllers.get(concludable);
    }

    static class RetrievableRequest<P extends Processor<ConceptMap, ?, ?, P>, C extends ConjunctionController<?, C, P>>
            extends Request<Retrievable, ConceptMap, RetrievableController, ConceptMap, P, C, RetrievableRequest<P, C>> {

        public RetrievableRequest(Driver<P> recProcessor, long recEndpointId, Retrievable provControllerId, ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Builder<ConceptMap, ConceptMap, RetrievableRequest<P, C>, P, ?> getBuilder(C controller) {
            ResolverView.FilteredRetrievable retrievableView = controller.retrievableProvider(pubControllerId());
            ConceptMap newPID = pubProcessorId().filter(retrievableView.filter());
            return new Builder<>(retrievableView.controller(), this)
                    .withMap(c -> merge(c, pubProcessorId()))
                    .withNewProcessorId(newPID);
        }
    }

    static class ConcludableRequest<P extends Processor<ConceptMap, ?, ?, P>, C extends ConjunctionController<?, C, P>>
            extends Request<Concludable, ConceptMap, ConcludableController, ConceptMap, P, C, ConcludableRequest<P, C>> {

        public ConcludableRequest(Driver<P> recProcessor, long recEndpointId, Concludable provControllerId, ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Builder<ConceptMap, ConceptMap, ConcludableRequest<P, C>, P, ?> getBuilder(C controller) {
            ResolverView.MappedConcludable controllerView = controller.concludableProvider(pubControllerId());
            Mapping mapping = Mapping.of(controllerView.mapping());
            ConceptMap newPID = mapping.transform(pubProcessorId());
            return new Builder<>(controllerView.controller(), this)
                    .withMap(mapping::unTransform)
                    .withNewProcessorId(newPID);
        }
    }

//    static class NegatedRequest<P extends Processor<ConceptMap, ?, P>> extends Request<Negated, ConceptMap, NegatedController, ConceptMap, P, NegatedRequest<P>> {
//
//        public NegatedRequest(Driver<P> recProcessor, long recEndpointId, Negated provControllerId, ConceptMap provProcessorId) {
//            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
//        }
//
//        @Override
//        public Builder<ConceptMap, ConceptMap, NegatedRequest<P>, P, NegatedController> getBuilder(ControllerRegistry registry) {
//            return null;  // TODO: Get the retrievable controller from the registry. Apply the filter in the same way as the mapping for concludable.
//        }
//    }

    protected static ConceptMap merge(ConceptMap c1, ConceptMap c2) {
        Map<Variable.Retrievable, Concept> compounded = new HashMap<>(c1.concepts());
        compounded.putAll(c2.concepts());
        return new ConceptMap(compounded);
    }

    protected static abstract class ConjunctionProcessor<OUTPUT,
            CONTROLLER extends ConjunctionController<OUTPUT, CONTROLLER, PROCESSOR>,
            PROCESSOR extends Processor<ConceptMap, OUTPUT, CONTROLLER, PROCESSOR>>
            extends Processor<ConceptMap, OUTPUT, CONTROLLER, PROCESSOR> {
        protected final ConceptMap bounds;
        protected final List<Resolvable<?>> plan;

        protected ConjunctionProcessor(Driver<PROCESSOR> driver, Driver<CONTROLLER> controller,
                                       ConceptMap bounds, List<Resolvable<?>> plan, String name) {
            super(driver, controller, noOp(name), name);
            this.bounds = bounds;
            this.plan = plan;
        }

        protected InletEndpoint<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            InletEndpoint<ConceptMap> endpoint = createReceivingEndpoint();
            if (planElement.isRetrievable()) {
                requestConnection(new RetrievableRequest<>(driver(), endpoint.id(), planElement.asRetrievable(),
                                                           carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isConcludable()) {
                requestConnection(new ConcludableRequest<>(driver(), endpoint.id(), planElement.asConcludable(),
                                                           carriedBounds.filter(planElement.retrieves())));
            } else if (planElement.isNegated()) {
                throw TypeDBException.of(ILLEGAL_STATE);  // TODO: Not implemented yet
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
            return endpoint;
        }


    }

}
