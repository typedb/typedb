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

package com.vaticle.typedb.core.reasoner.controllers;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.reactive.CompoundReactive;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry.ResolverView;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.IdentityReactive.noOp;

public abstract class ConjunctionController<CONTROLLER extends Controller<Resolvable<?>, ConceptMap, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<ConceptMap, Resolvable<?>, PROCESSOR>> extends Controller<Resolvable<?>,
        ConceptMap, PROCESSOR, CONTROLLER> {

    protected final Conjunction conjunction;
    protected final ControllerRegistry registry;
    final Set<Resolvable<?>> resolvables;
    final Set<Negated> negateds;

    public ConjunctionController(Driver<CONTROLLER> driver, Conjunction conjunction,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")", executorService);
        this.conjunction = conjunction;
        this.resolvables = new HashSet<>();
        this.negateds = new HashSet<>();
        this.registry = registry;
        initialiseProviderControllers();
    }

    protected void initialiseProviderControllers() {
        Set<Concludable> concludables = concludablesTriggeringRules();
        Set<Retrievable> retrievables = Retrievable.extractFrom(conjunction, concludables);
        resolvables.addAll(concludables);
        resolvables.addAll(retrievables);
    }

    abstract Set<Concludable> concludablesTriggeringRules();

    protected List<Resolvable<?>> plan() {
        return new ArrayList<>(resolvables);
    }

    @Override
    protected ConnectionBuilder<Resolvable<?>, ConceptMap, ?, ?> getProviderController(ConnectionRequest<Resolvable<?>, ConceptMap, ?> connectionRequest) {
        Resolvable<?> pubCID = connectionRequest.pubControllerId();
        if (pubCID.isRetrievable()) {
            ResolverView.FilteredRetrievable controller = registry.registerRetrievableController(pubCID.asRetrievable());
            ConceptMap newPID = connectionRequest.pubProcessorId().filter(controller.filter());
            return connectionRequest.createConnectionBuilder(controller.controller())
                    .withMap(c -> merge(c, connectionRequest.pubProcessorId()))
                    .withNewProcessorId(newPID);
        } else if (pubCID.isConcludable()) {
            ResolverView.MappedConcludable controllerView = registry.registerConcludableController(pubCID.asConcludable());
            Driver<ConcludableController> controller = controllerView.controller();
            Mapping mapping = Mapping.of(controllerView.mapping());
            ConceptMap newPID = mapping.transform(connectionRequest.pubProcessorId());
            return connectionRequest.createConnectionBuilder(controller)
                    .withMap(mapping::unTransform)
                    .withNewProcessorId(newPID);
        } else if (pubCID.isNegated()) {
            return null;  // TODO: Get the retrievable controller from the registry. Apply the filter in the same way as the mapping for concludable.
        }
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static ConceptMap merge(ConceptMap c1, ConceptMap c2) {
        Map<Variable.Retrievable, Concept> compounded = new HashMap<>(c1.concepts());
        compounded.putAll(c2.concepts());
        return new ConceptMap(compounded);
    }

    public static class ConjunctionProcessor<PROCESSOR extends Processor<ConceptMap, Resolvable<?>, PROCESSOR>> extends Processor<ConceptMap, Resolvable<?>, PROCESSOR> {
        private final ConceptMap bounds;
        private final List<Resolvable<?>> plan;

        protected ConjunctionProcessor(Driver<PROCESSOR> driver,
                                       Driver<? extends Controller<Resolvable<?>, ConceptMap, PROCESSOR, ?>> controller,
                                       String name, ConceptMap bounds, List<Resolvable<?>> plan) {
            super(driver, controller, name, noOp());
            this.bounds = bounds;
            this.plan = plan;
        }

        @Override
        public void setUp() {
            new CompoundReactive<>(plan, this::nextCompoundLeader, ConjunctionController::merge, bounds).publishTo(outlet());
        }

        private InletEndpoint<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            return requestConnection(driver(), planElement, carriedBounds.filter(planElement.retrieves()));
        }
    }

}
