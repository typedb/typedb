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
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
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
    private Actor.Driver<? extends AbstractReactiveBlock<?, ?, ?, ?>> rootReactiveBlock;
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
        this.controllerRegistry.registerRootConjunctionController(conjunction, filter, this);  // TODO: Doesn't indicate that this also triggers the setup of the upstream controllers and creates a reactiveBlock and connects it back to this producer. Clean up this storyline.
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
        this.controllerRegistry.registerRootDisjunctionController(disjunction, filter, this);
        if (options.traceInference()) {
            Tracer.initialise(options.reasonerDebuggerDir());
            Tracer.get().startDefaultTrace();
        }
    }

    @Override
    public void initialise(Actor.Driver<? extends AbstractReactiveBlock<?, ?, ?, ?>> rootReactiveBlock) {
        assert this.rootReactiveBlock == null;
        this.rootReactiveBlock = rootReactiveBlock;
        if (required.get() > 0) pull();
    }

    @Override
    public synchronized void produce(Queue<ConceptMap> queue, int request, Executor executor) {
        assert this.queue == null || this.queue == queue;
        assert request > 0;
        this.queue = queue;
        required.addAndGet(request); // TODO: improve variable naming here for required and request
        if (rootReactiveBlock != null && !isPulling) pull();
    }

    @Override
    public void receiveAnswer(ConceptMap answer) {
        isPulling = false;
        // if (options.traceInference()) Tracer.get().finishDefaultTrace();
        // TODO: The explainables can always be given
        if (options.explain() && !answer.explainables().isEmpty()) {
            // TODO: Explainables recorded here
            explainablesManager.setAndRecordExplainables(answer);
        }
        queue.put(answer);
        if (required.decrementAndGet() > 0) pull();
    }

    private void pull() {
        assert rootReactiveBlock != null;
        isPulling = true;
        rootReactiveBlock.execute(actor -> actor.rootPull());
    }

    @Override
    public void finished() {
        // note: root resolver calls this single-threaded, so is thread safe
        LOG.trace("All answers found.");
//        if (options.traceInference()) Tracer.get().finishDefaultTrace();
        finish();
    }

    private void finish() {
        if (!done) {
            done = true;
            required.set(0);
            queue.done();
        }
    }

    @Override
    public void exception(Throwable e) {
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
