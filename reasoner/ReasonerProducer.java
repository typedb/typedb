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
 */

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.controller.Registry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class ReasonerProducer implements Producer<ConceptMap>, ReasonerConsumer { // TODO: Rename to MatchProducer and create abstract supertype

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final AtomicInteger required;
    private final Options.Query options;
    private final Registry controllerRegistry;
    private final ExplainablesManager explainablesManager;
    private boolean done;
    private Queue<ConceptMap> queue;
    private Actor.Driver<? extends Processor<?, ?, ?, ?>> rootProcessor;
    private boolean isPulling;

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    public ReasonerProducer(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            Registry controllerRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = false;
        this.required = new AtomicInteger();
        this.isPulling = false;
        this.controllerRegistry.createRootConjunctionController(conjunction, filter, this);
        if (options.traceInference()) {
            Tracer.initialise(options.reasonerDebuggerDir());
            Tracer.get().startDefaultTrace();
        }
    }

    public ReasonerProducer(Disjunction disjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            Registry controllerRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = false;
        this.required = new AtomicInteger();
        this.isPulling = false;
        this.controllerRegistry.createRootDisjunctionController(disjunction, filter, this);
        if (options.traceInference()) {
            Tracer.initialise(options.reasonerDebuggerDir());
            Tracer.get().startDefaultTrace();
        }
    }

    @Override
    public void setRootProcessor(Actor.Driver<? extends Processor<?, ?, ?, ?>> rootProcessor) {
        this.rootProcessor = rootProcessor;
        if (required.get() > 0) pull();
    }

    @Override
    public synchronized void produce(Queue<ConceptMap> queue, int request, Executor executor) {
        assert this.queue == null || this.queue == queue;
        assert request > 0;
        this.queue = queue;
        required.addAndGet(request);
        if (!isPulling) pull();
    }

    @Override
    public void receiveAnswer(ConceptMap answer) {
        isPulling = false;
        // if (options.traceInference()) Tracer.get().finishDefaultTrace();
        if (options.explain() && !answer.explainables().isEmpty()) {
            explainablesManager.setAndRecordExplainables(answer);
        }
        queue.put(answer);
        if (required.decrementAndGet() > 0) pull();
    }

    private void pull() {
        if (rootProcessor != null) {
            isPulling = true;
            rootProcessor.execute(actor -> actor.entryPull());
        }
    }

    @Override
    public void answersFinished() {
        // note: root resolver calls this single-threaded, so is thread safe
        LOG.trace("All answers found.");
//        if (options.traceInference()) Tracer.get().finishDefaultTrace();
        finish();
    }

    private void finish() {
        if (!done) {
            done = true;
            queue.done();
            required.set(0);
        }
    }

    @Override
    public void exception(Throwable e) {
        // TODO: Should this mirror finish()?
        if (!done) {
            done = true;
            required.set(0);
            queue.done(e);
        }
    }

    @Override
    public void recycle() {

    }

}
