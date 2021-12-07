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
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.reasoner.compute.Controller;
import com.vaticle.typedb.core.reasoner.compute.Processor;
import com.vaticle.typedb.core.reasoner.reactive.MapReactive;
import com.vaticle.typedb.core.reasoner.reactive.Publisher;
import com.vaticle.typedb.core.reasoner.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.controllers.ConcludableController.ConcludableAns;
import com.vaticle.typedb.core.reasoner.controllers.ConcludableController.ConcludableProcessor;
import com.vaticle.typedb.core.reasoner.compute.Processor.Connection.Builder;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.reactive.CompoundReactive.compound;

public class ConjunctionController extends Controller<Conjunction, ConceptMap, ConjunctionController.ConjunctionAns, ConjunctionController.ConjunctionProcessor, ConjunctionController> {

    protected ConjunctionController(Driver<ConjunctionController> driver, String name, Conjunction id, ActorExecutorGroup executorService) {
        super(driver, name, id, executorService);
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
    protected <PUB_CID, PUB_PID, PACKET, PUB_CONTROLLER extends Controller<PUB_CID, PUB_PID, PACKET, PUB_PROCESSOR,
            PUB_CONTROLLER>, PUB_PROCESSOR extends Processor<PACKET, PUB_PROCESSOR>>
    Driver<PUB_CONTROLLER> getControllerForId(PUB_CID pub_cid) {
        if (pub_cid instanceof Retrievable) {
            return null;  // Get the retrievable controller from the registry
        } else if (pub_cid instanceof Concludable) {
            return null;  // Get the concludable controller from the registry
        } else if (pub_cid instanceof Negation) {
            return null;  // Get the negation controller from the registry
        }
        throw TypeDBException.of(ILLEGAL_STATE);
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
                Reactive<ConcludableAns, ConceptMap> op = new MapReactive<>(set(), set(), ConcludableAns::conceptMap);  // TODO: Now this doesn't know the type because the mapping is declared in the wrong direction for that :\
                if (planElement.isConcludable()) {
                    // TODO: It's kind of lucky we need a mapping to be done here otherwise we'd need a meaningless reactive here to give to the connection builder because the connection isn't established straight away.
                    Builder<ConcludableAns, ConjunctionProcessor, Concludable, ConceptMap, ConcludableProcessor>
                            connectionBuilder = new Builder<>(driver(), planElement.asConcludable(), filteredBounds, op);
                    requestConnection(connectionBuilder);
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
            op.subscribe(compound(set(op), plan, spawnLeaderFunc, bounds, compoundPacketsFunc));
        }
    }

}
