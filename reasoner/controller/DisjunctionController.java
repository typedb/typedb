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

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;
import static com.vaticle.typedb.core.reasoner.controller.ConjunctionController.merge;

public abstract class DisjunctionController<CONTROLLER extends Controller<ConceptMap, ConceptMap, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<ConceptMap, ConceptMap, PROCESSOR>> extends Controller<ConceptMap, ConceptMap, PROCESSOR, CONTROLLER> {

    protected final Disjunction disjunction;

    protected DisjunctionController(Driver<CONTROLLER> driver, Disjunction disjunction, ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, DisjunctionController.class.getSimpleName() + "(pattern:" + disjunction + ")", executorService, registry);
        this.disjunction = disjunction;
    }

    private static class NestedConjunctionRequest<P extends Processor<ConceptMap, ?, P>>
            extends Connection.Request<Conjunction, ConceptMap, NestedConjunctionController, ConceptMap, P, NestedConjunctionRequest<P>> {
        protected NestedConjunctionRequest(Driver<P> recProcessor, long recEndpointId, Conjunction provControllerId,
                                           ConceptMap provProcessorId) {
            super(recProcessor, recEndpointId, provControllerId, provProcessorId);
        }

        @Override
        public Connection.Builder<ConceptMap, ConceptMap, NestedConjunctionRequest<P>, P, ?> getBuilder(ControllerRegistry registry) {
            // TODO: Conjunction equality can prove to be a real problem here when using it to uniquely refer to conjunctionControllers in the registry
            // TODO: In fact we shouldn't be looking up the controller more than once.
            return createConnectionBuilder(registry.nestedConjunctionController(pubControllerId()))
                    .withMap(c -> merge(c, pubProcessorId()));
        }
        // TODO: Copy over the PROCESSOR type from a conjuntion type somewhere
    }

    protected static abstract class DisjunctionProcessor<PROCESSOR extends Processor<ConceptMap, ConceptMap, PROCESSOR>>
            extends Processor<ConceptMap, ConceptMap, PROCESSOR> {

        private final Disjunction disjunction;
        private final ConceptMap bounds;

        protected DisjunctionProcessor(Driver<PROCESSOR> driver,
                                       Driver<? extends Controller<?, ?, PROCESSOR, ?>> controller,
                                       Disjunction disjunction, ConceptMap bounds, String name) {
            super(driver, controller, noOp(name), name);
            this.disjunction = disjunction;
            this.bounds = bounds;
        }

        @Override
        public void setUp() {
            for (com.vaticle.typedb.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
                InletEndpoint<ConceptMap> endpoint = createReceivingEndpoint();
                endpoint.publishTo(outlet());
                Set<Retrievable> retrievableConjunctionVars = iterate(conjunction.variables())
                        .map(Variable::id).filter(Identifier::isRetrievable)
                        .map(Identifier.Variable::asRetrievable).toSet();
                requestConnection(new NestedConjunctionRequest<>(driver(), endpoint.id(), conjunction, bounds.filter(retrievableConjunctionVars)));
            }
        }
    }
}
