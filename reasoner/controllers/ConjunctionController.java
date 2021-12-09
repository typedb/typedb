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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionBuilder;
import com.vaticle.typedb.core.reasoner.reactive.IdentityReactive;
import com.vaticle.typedb.core.reasoner.reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.CompoundReactive.compound;

public class ConjunctionController extends Controller<Conjunction, ConceptMap, ConceptMap, Resolvable<?>, ConceptMap, ConjunctionController.ConjunctionAns, ConjunctionController.ConjunctionProcessor, ConjunctionController> {

    private final ResolverRegistry registry;

    protected ConjunctionController(Driver<ConjunctionController> driver, String name, Conjunction id, ActorExecutorGroup executorService, ResolverRegistry registry) {
        super(driver, name, id, executorService);
        this.registry = registry;
    }

    @Override
    protected Function<Driver<ConjunctionProcessor>, ConjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        List<Resolvable<?>> plan = plan(id());
        return driver -> new ConjunctionProcessor(driver, driver(), "name", bounds, plan);
    }

    private List<Resolvable<?>> plan(Conjunction conjunction) {
        return null;  // TODO
    }

    @Override
    protected ConnectionBuilder<ConceptMap, ConceptMap, ?> getPublisherController(ConnectionBuilder connectionBuilder) {
        Resolvable<?> pub_cid = connectionBuilder.publisherControllerId();
        if (pub_cid.isRetrievable()) {
            return null;  // TODO: Get the retrievable controller from the registry. Apply the filter in the same way as the mapping for concludable.
        } else if (pub_cid.isConcludable()) {
            Pair<Driver<ConcludableController>, Map<Variable.Retrievable, Variable.Retrievable>> pair = registry.registerConcludableController((Concludable) pub_cid);
            Driver<ConcludableController> controller = pair.first();
            Mapping mapping = Mapping.of(pair.second());
            ConceptMap newPID = mapping.transform(connectionBuilder.publisherProcessorId());
//            Reactive<ConceptMap, ConceptMap> newOp = connectionBuilder.subscriber().mapSubscribe(mapping::unTransform);
            Function<ConceptMap, ConceptMap> fn = mapping::unTransform;
            return connectionBuilder.addPublisherController(controller).mapSubscribe(newPID, fn);
        } else if (pub_cid.isNegated()) {
            return null;  // TODO: Get the retrievable controller from the registry. Apply the filter in the same way as the mapping for concludable.
        }
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected Driver<ConjunctionProcessor> addConnectionPubProcessor(ConnectionBuilder<ConceptMap, ConjunctionAns, ?> connectionBuilder) {
        // TODO: This is where we can do subsumption
        return processors.computeIfAbsent(connectionBuilder.publisherProcessorId(), this::buildProcessor);
    }

    public static class ConjunctionAns {

        ConjunctionAns(ConceptMap conceptMap) {
            // TODO
        }

        public ConceptMap conceptMap() {
            return null;  // TODO
        }
    }

    public static class ConjunctionProcessor extends Processor<ConceptMap, Resolvable<?>, ConceptMap, ConjunctionAns, ConjunctionProcessor> {

        protected ConjunctionProcessor(Driver<ConjunctionProcessor> driver, Driver<? extends Controller<?, ?,
                ConceptMap, Resolvable<?>, ConceptMap, ConjunctionAns, ConjunctionProcessor, ?>> controller,
                                       String name, ConceptMap bounds, List<Resolvable<?>> plan) {
            super(driver, controller, name, new Outlet.Single<>());

            BiFunction<Resolvable<?>, ConceptMap, Publisher<ConceptMap>> spawnLeaderFunc = (planElement, carriedBounds) -> {
                ConceptMap filteredBounds = carriedBounds.filter(planElement.retrieves());
                IdentityReactive<ConceptMap> op = IdentityReactive.noOp(set(), set());  // No-op so that the connection has an op to connect to
                endpoint = requestConnection(driver(), planElement, filteredBounds);
                return op;
            };

            BiFunction<ConceptMap, ConceptMap, ConceptMap> compoundPacketsFunc = (c1, c2) -> {
                Map<Variable.Retrievable, Concept> compounded = new HashMap<>(c1.concepts());
                compounded.putAll(c2.concepts());
                return new ConceptMap(compounded);
            };

            Reactive<ConceptMap, ConjunctionAns> op = outlet().mapSubscribe(ConjunctionAns::new);
            op.subscribe(compound(set(op), plan, bounds, spawnLeaderFunc, compoundPacketsFunc));
        }
    }

}
