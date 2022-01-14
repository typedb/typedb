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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.reasoner.computation.reactive.IdentityReactive.noOp;
import static com.vaticle.typedb.core.reasoner.controller.ConjunctionController.merge;

public abstract class DisjunctionController<CONTROLLER extends Controller<ConceptMap, ConceptMap, ConceptMap, PROCESSOR, CONTROLLER>,
        PROCESSOR extends Processor<ConceptMap, ConceptMap, PROCESSOR>> extends Controller<ConceptMap, ConceptMap, ConceptMap, PROCESSOR, CONTROLLER> {

    private final List<Pair<Conjunction, Driver<NestedConjunctionController>>> conjunctionControllers;
    protected Disjunction disjunction;

    protected DisjunctionController(Driver<CONTROLLER> driver, Disjunction disjunction, ActorExecutorGroup executorService, ControllerRegistry registry) {
        super(driver, DisjunctionController.class.getSimpleName() + "(pattern:" + disjunction + ")", executorService, registry);
        this.disjunction = disjunction;
        this.conjunctionControllers = new ArrayList<>();
        disjunction.conjunctions().forEach(conjunction -> {
            Driver<NestedConjunctionController> controller = registry().nestedConjunctionController(conjunction);
            conjunctionControllers.add(new Pair<>(conjunction, controller));
        });
    }

    private Driver<NestedConjunctionController> getConjunctionController(Conjunction conjunction) {
        // TODO: Only necessary because conjunction equality is not well defined
        Optional<Driver<NestedConjunctionController>> controller =
                iterate(conjunctionControllers).filter(p -> p.first() == conjunction).map(Pair::second).first();
        if (controller.isPresent()) return controller.get();
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, ConceptMap, PROCESSOR, REQ>,
            PUB_C extends Controller<PUB_PROC_ID, ?, ConceptMap, ?, PUB_C>> void findProviderForConnection(REQ req) {
        Connection.Builder<PUB_PROC_ID, ConceptMap, ?, ?, ?> builder = createBuilder(req);
        builder.providerController().execute(actor -> actor.makeConnection(builder));
    }

    @Override
    protected <PUB_CID, PUB_PROC_ID, REQ extends Processor.Request<PUB_CID, PUB_PROC_ID, PUB_C, ConceptMap, PROCESSOR,
            REQ>, PUB_C extends Controller<PUB_PROC_ID, ?, ConceptMap, ?, PUB_C>> Connection.Builder<PUB_PROC_ID,
            ConceptMap, ?, ?, ?> createBuilder(REQ req) {
        PUB_CID cid = req.pubControllerId();
        Connection.Builder<PUB_PROC_ID, ConceptMap, ?, ?, ?> builder;
        if (cid instanceof Conjunction) {
            Driver<PUB_C> conjunctionController = (Driver<PUB_C>) getConjunctionController((Conjunction) req.pubControllerId());
            builder = new Connection.Builder<PUB_PROC_ID, ConceptMap, REQ, PROCESSOR, PUB_C>(conjunctionController, req).withMap(c -> merge(c, (ConceptMap) req.pubProcessorId()));
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
        return builder;
    }

    protected static abstract class DisjunctionProcessor<PROCESSOR extends Processor<ConceptMap, ConceptMap, PROCESSOR>>
            extends Processor<ConceptMap, ConceptMap, PROCESSOR> {

        private final Disjunction disjunction;
        private final ConceptMap bounds;

        protected DisjunctionProcessor(Driver<PROCESSOR> driver,
                                       Driver<? extends Controller<?, ConceptMap, ?, PROCESSOR, ?>> controller,
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

        private static class NestedConjunctionRequest<P extends Processor<ConceptMap, ?, P>>
                extends Request<Conjunction, ConceptMap, NestedConjunctionController, ConceptMap, P, NestedConjunctionRequest<P>> {
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
    }
}
