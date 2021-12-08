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
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest1;
import com.vaticle.typedb.core.reasoner.compute.Processor.ConnectionRequest2;
import com.vaticle.typedb.core.reasoner.controllers.ConcludableController.ConcludableAns;
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
import static com.vaticle.typedb.core.reasoner.reactive.MapReactive.map;

public class ConjunctionController extends Controller<Conjunction, ConceptMap, ConjunctionController.ConjunctionAns, ConjunctionController.ConjunctionProcessor, ConjunctionController> {

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
    protected <
            PUB_CID, PUB_PID, PACKET, PUB_CONTROLLER extends Controller<PUB_CID, PUB_PID, PACKET, PUB_PROCESSOR,
            PUB_CONTROLLER>, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>>
    ConnectionRequest2<PUB_PID, PACKET, ConjunctionProcessor, PUB_CONTROLLER> addConnectionPubController(
            ConnectionRequest1<PUB_CID, PUB_PID, PACKET, ConjunctionProcessor> connectionBuilder) {
        // TODO: Now we have the builder we can add the controller for the publisher we want and also any transformations

        PUB_CID pub_cid = connectionBuilder.publisherControllerId();

        if (pub_cid instanceof Retrievable) {
            return null;  // Get the retrievable controller from the registry
        } else if (pub_cid instanceof Concludable) {
            // TODO: Would like this to reflect the pattern in Concludable, where we know all of the transformations ahead of time, but it's not possible because we're in the wrong controller for that
            // TODO: Instead we could keep this more flexible and pass in a partly built
            Pair<Driver<ConcludableController>, Map<Variable.Retrievable, Variable.Retrievable>> pair =
                    registry.registerConcludableController((Concludable) pub_cid);  // Get the concludable controller from the registry
            Driver<PUB_CONTROLLER> controller = (Driver<PUB_CONTROLLER>) pair.first();
            Mapping mapping = Mapping.of(pair.second());
            ConceptMap pid = (ConceptMap) connectionBuilder.publisherProcessorId();  // TODO: Extra casting
            ConceptMap newPID = mapping.transform(pid);
            Function<ConceptMap, ConceptMap> fn = mapping::unTransform;
            Reactive<R, PACKET> newOp = connectionBuilder.subscriber().mapSubscribe(fn);
            connectionBuilder.withPublisherController(controller);
        } else if (pub_cid instanceof Negation) {
            return null;  // Get the negation controller from the registry
        }
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected Driver<ConjunctionProcessor> addConnectionPubProcessor(ConnectionRequest2<ConceptMap,
            ConjunctionAns, ?, ?> connectionBuilder) {
        // TODO: This is where we can do subsumption
        processor = processors.computeIfAbsent(builder, c -> buildProcessor(builder));
    }

    public static class ConjunctionAns {

        ConjunctionAns(ConceptMap conceptMap) {
            // TODO
        }

        public ConceptMap conceptMap() {
            return null;  // TODO
        }
    }

    public static class ConjunctionProcessor extends Processor<ConjunctionAns, ConjunctionProcessor> {

        protected ConjunctionProcessor(Driver<ConjunctionProcessor> driver, Driver<? extends Controller<?, ?,
                ConjunctionAns, ConjunctionProcessor, ?>> controller, String name, ConceptMap bounds, List<Resolvable<?>> plan) {
            super(driver, controller, name, new Outlet.Single<>());

            // TODO: The mapping (in fact the untransform() step) should be included in the connection
            // TODO: How are we going to carry around the full partial answer, not just the part needed for this resolvable?
            // TODO: Get the mapping from the variables of the resolvable in the conjunction to the variables in the processor we'll use

            BiFunction<Resolvable<?>, ConceptMap, Publisher<ConceptMap>> spawnLeaderFunc = (planElement, carriedBounds) -> {
                ConceptMap filteredBounds = carriedBounds.filter(planElement.retrieves());
                Reactive<ConcludableAns, ConceptMap> op = map(set(), set(), ConcludableAns::conceptMap);  // TODO: Now this doesn't know the type because the mapping is declared in the wrong direction for that :\
                if (planElement.isConcludable()) {
                    // TODO: It's kind of lucky we need a mapping to be done here otherwise we'd need a meaningless reactive here to give to the connection builder because the connection isn't established straight away.
//                    IdentityReactive<ConcludableAns> connectionPort = IdentityReactive.identity(set(), set()); // TODO Add this to concludable
                    requestConnection(driver(), op, filteredBounds, planElement.asConcludable());
                } else if (planElement.isRetrievable()) {
                    // TODO
                } else if (planElement.isNegated()) {
                    // TODO
                }
                return op;
            };

            BiFunction<ConceptMap, ConceptMap, ConceptMap> compoundPacketsFunc = (c1, c2) -> {
                Map<Variable.Retrievable, Concept>  compounded = new HashMap<>(c1.concepts());
                compounded.putAll(c2.concepts());
                return new ConceptMap(compounded);
            };

            Reactive<ConceptMap, ConjunctionAns> op = outlet().mapSubscribe(ConjunctionAns::new);
            op.subscribe(compound(set(op), plan, bounds, spawnLeaderFunc, compoundPacketsFunc));
        }
    }

}
