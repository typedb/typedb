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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.CompoundReactive.compound;
import static com.vaticle.typedb.core.reasoner.reactive.IdentityReactive.noOp;

public abstract class ConjunctionController<CONTROLLER extends Controller<Resolvable<?>, ConceptMap, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<ConceptMap, Resolvable<?>, PROCESSOR>> extends Controller<Resolvable<?>,
        ConceptMap, PROCESSOR, CONTROLLER> {

    protected final Conjunction conjunction;
    private final ControllerRegistry registry;

    public ConjunctionController(Driver<CONTROLLER> driver, Conjunction conjunction,
                                 ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, ConjunctionController.class.getSimpleName() + "(pattern:" + conjunction + ")", executorService);
        this.conjunction = conjunction;
        this.registry = registry;
    }

    protected List<Resolvable<?>> plan(Conjunction conjunction) {
        return null;  // TODO
    }

    @Override
    protected ConnectionBuilder<Resolvable<?>, ConceptMap, ?, ?> getPublisherController(ConnectionRequest<Resolvable<?>, ConceptMap, ?> connectionRequest) {
        Resolvable<?> pub_cid = connectionRequest.pubControllerId();
        if (pub_cid.isRetrievable()) {
            return null;  // TODO: Get the retrievable controller from the registry. Apply the filter in the same way as the mapping for concludable.
        } else if (pub_cid.isConcludable()) {
            Pair<Driver<ConcludableController>, Map<Variable.Retrievable, Variable.Retrievable>> pair = registry.registerConcludableController(pub_cid.asConcludable());
            Driver<ConcludableController> controller = pair.first();
            Mapping mapping = Mapping.of(pair.second());
            ConceptMap newPID = mapping.transform(connectionRequest.pubProcessorId());
            return connectionRequest.createConnectionBuilder(controller).withMap(newPID, mapping::unTransform);
        } else if (pub_cid.isNegated()) {
            return null;  // TODO: Get the retrievable controller from the registry. Apply the filter in the same way as the mapping for concludable.
        }
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected Driver<PROCESSOR> computeProcessorIfAbsent(ConnectionBuilder<?, ConceptMap, ?, ?> connectionBuilder) {
        // TODO: This is where we can do subsumption
        return processors.computeIfAbsent(connectionBuilder.publisherProcessorId(), this::buildProcessor);
    }

    public static class ConjunctionProcessor<PROCESSOR extends Processor<ConceptMap, Resolvable<?>, PROCESSOR>> extends Processor<ConceptMap, Resolvable<?>, PROCESSOR> {
        protected ConjunctionProcessor(Driver<PROCESSOR> driver,
                                       Driver<? extends Controller<Resolvable<?>, ConceptMap, PROCESSOR, ?>> controller,
                                       String name, ConceptMap bounds, List<Resolvable<?>> plan) {
            super(driver, controller, name, noOp());
            compound(plan, bounds, this::nextCompoundLeader, ConjunctionProcessor::merge).publishTo(noOp());
        }

        private InletEndpoint<ConceptMap> nextCompoundLeader(Resolvable<?> planElement, ConceptMap carriedBounds) {
            return requestConnection(driver(), planElement, carriedBounds.filter(planElement.retrieves()));
        }

        private static ConceptMap merge(ConceptMap c1, ConceptMap c2) {
            Map<Variable.Retrievable, Concept> compounded = new HashMap<>(c1.concepts());
            compounded.putAll(c2.concepts());
            return new ConceptMap(compounded);
        }
    }

}
