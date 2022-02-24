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

import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.ReasonerProducer.EntryPoint;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.stream.CompoundStream;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.set;

public class RootConjunctionController extends ConjunctionController<ConceptMap, RootConjunctionController, RootConjunctionController.RootConjunctionProcessor> {
    private final Set<Identifier.Variable.Retrievable> filter;
    private final EntryPoint reasonerEndpoint;

    public RootConjunctionController(Driver<RootConjunctionController> driver, Conjunction conjunction,
                                     Set<Identifier.Variable.Retrievable> filter, ActorExecutorGroup executorService, Registry registry,
                                     EntryPoint reasonerEndpoint) {
        super(driver, conjunction, executorService, registry);
        this.filter = filter;
        this.reasonerEndpoint = reasonerEndpoint;
    }

    @Override
    protected Function<Driver<RootConjunctionProcessor>, RootConjunctionProcessor> createProcessorFunc(ConceptMap bounds) {
        return driver -> new RootConjunctionProcessor(
                driver, driver(), bounds, plan(), filter, reasonerEndpoint,
                RootConjunctionProcessor.class.getSimpleName() + "(pattern:" + conjunction + ", bounds: " + bounds + ")"
        );
    }

    @Override
    public RootConjunctionController asController() {
        return this;
    }

    @Override
    Set<Concludable> concludablesTriggeringRules() {
        return Iterators.iterate(Concludable.create(conjunction))
                .filter(c -> c.getApplicableRules(registry().conceptManager(), registry().logicManager()).hasNext())
                .toSet();
    }

    @Override
    protected void exception(Throwable e) {
        super.exception(e);
        reasonerEndpoint.exception(e);
    }

    protected static class RootConjunctionProcessor extends ConjunctionController.ConjunctionProcessor<ConceptMap, RootConjunctionController, RootConjunctionProcessor> {

        private final Set<Identifier.Variable.Retrievable> filter;
        private final EntryPoint reasonerEntryPoint;

        protected RootConjunctionProcessor(Driver<RootConjunctionProcessor> driver,
                                           Driver<RootConjunctionController> controller, ConceptMap bounds,
                                           List<Resolvable<?>> plan, Set<Identifier.Variable.Retrievable> filter,
                                           EntryPoint reasonerEntryPoint, String name) {
            super(driver, controller, bounds, plan, name);
            this.filter = filter;
            this.reasonerEntryPoint = reasonerEntryPoint;
            this.reasonerEntryPoint.setMonitor(monitoring());
        }

        @Override
        protected Monitoring createMonitoring() {
            return new Monitor(this);
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
            super.setUp();
            outlet().publishTo(reasonerEntryPoint);
            new CompoundStream<>(plan, this::nextCompoundLeader, ConjunctionController::merge, bounds, monitoring(), name())
                    .buffer()
                    .map(conceptMap -> conceptMap.filter(filter))
                    .deduplicate()
                    .publishTo(outlet());
        }

        @Override
        protected boolean isPulling() {
            return reasonerEntryPoint.isPulling();
        }

        @Override
        protected void onDone() {
            assert ! done;
//            done = true;
            reasonerEntryPoint.done();
        }
    }
}
