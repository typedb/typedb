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
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.computation.actor.Controller;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.ReactiveStreamBase;

import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.reasoner.computation.reactive.NoOpReactive.noOp;

public class NegationController extends Controller<ConceptMap, ConceptMap, ConceptMap, NegationController.NegationProcessor, NegationController> {

    private final Negated negated;
    private Driver<NestedDisjunctionController> disjunctionContoller;

    public NegationController(Driver<NegationController> driver, Negated negated, ActorExecutorGroup executorService,
                              Registry registry) {
        super(driver, executorService, registry, NegationController.class.getSimpleName() + "(pattern:" + negated + ")");
        this.negated = negated;
    }

    @Override
    public void setUpUpstreamProviders() {
        // TODO: If there is only one conjunction in the disjunction we could theoretically skip the disjunction, but
        //  this is architecturally difficult.
        disjunctionContoller = registry().registerNestedDisjunctionController(negated.pattern());
    }

    @Override
    protected Function<Driver<NegationProcessor>, NegationProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new NegationProcessor(
                driver, driver(), negated, bounds,
                NegationProcessor.class.getSimpleName() + "(pattern: " + negated + ", bounds: " + bounds + ")"
        );
    }

    private Driver<NestedDisjunctionController> disjunctionContoller() {
        return disjunctionContoller;
    }

    @Override
    public NegationController asController() {
        return this;
    }

    protected static class NegationProcessor extends Processor<ConceptMap, ConceptMap, NegationController, NegationProcessor> {

        private final Negated negated;
        private final ConceptMap bounds;
        private NegationReactive negation;

        protected NegationProcessor(Driver<NegationProcessor> driver, Driver<NegationController> controller,
                                    Negated negated, ConceptMap bounds, String name) {
            super(driver, controller, name);
            this.negated = negated;
            this.bounds = bounds;
        }

        @Override
        protected Monitoring createMonitoring() {
            return new Monitor(this);
        }

        @Override
        protected boolean isPulling() {
            return true;  // TODO: Check this
        }

        @Override
        protected Set<Driver<? extends Processor<?, ?, ?, ?>>> upstreamMonitors() {
            return set(driver());
        }

        @Override
        protected Set<Driver<? extends Processor<?, ?, ?, ?>>> newUpstreamMonitors(Set<Driver<? extends Processor<?, ?, ?, ?>>> monitors) {
            return set(driver());
        }

        @Override
        public void setUp() {
            setOutlet(noOp(monitoring(), name()));
            InletEndpoint<ConceptMap> endpoint = createReceivingEndpoint();
            requestConnection(new DisjunctionRequest(driver(), endpoint.id(), negated.pattern(), bounds));
            negation = new NegationReactive(monitoring(), name());
            endpoint.publishTo(negation);
            negation.publishTo(outlet());
        }

        @Override
        protected void onDone() {
            assert !done;
//            done = true;
            negation.receiveDone(bounds);
            monitoring().reportPathJoin(negation);
        }

        private static class NegationReactive extends ReactiveStreamBase<ConceptMap, ConceptMap> {

            private boolean answerFound;

            protected NegationReactive(Monitoring monitor, String groupName) {
                super(monitor, groupName);
                answerFound = false;
            }

            @Override
            protected Manager<ConceptMap> providerManager() {
                return new SingleManager<>(this, monitor());
            }

            @Override
            public void receive(Provider<ConceptMap> provider, ConceptMap packet) {
                super.receive(provider, packet);
                answerFound = true;
                monitor().onAnswerDestroy(this);
            }

            public void receiveDone(ConceptMap packet) {
                if (!answerFound) {
                    monitor().reportAnswerCreate(this);  // TODO: This increases the local count of answers as well as the desired effect of informing the root. Consider manually terminating the monitor? Not good for using assertions to ensure answers weren't missed though
                    subscriber().receive(this, packet);
                }
            }
        }

        protected static class DisjunctionRequest extends Request<Disjunction, ConceptMap, NestedDisjunctionController,
                ConceptMap, NegationProcessor, NegationController, DisjunctionRequest> {

            protected DisjunctionRequest(Driver<NegationProcessor> recProcessor, long recEndpointId,
                                         Disjunction provControllerId, ConceptMap provProcessorId) {
                super(recProcessor, recEndpointId, provControllerId, provProcessorId);
            }

            @Override
            public Builder<ConceptMap, ConceptMap, DisjunctionRequest, NegationProcessor, ?> getBuilder(NegationController controller) {
                return new Builder<>(controller.disjunctionContoller(), this);
            }
        }
    }

}
