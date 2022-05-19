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
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.controller.Registry;
import com.vaticle.typedb.core.reasoner.reactive.AbstractReactiveBlock;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public abstract class ReasonerProducer<ANSWER> implements Producer<ANSWER>, ReasonerConsumer<ANSWER> {

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final Registry controllerRegistry;
    private final AtomicBoolean done;
    final AtomicInteger requiredAnswers;
    final Options.Query options;
    final ExplainablesManager explainablesManager;
    private Actor.Driver<? extends AbstractReactiveBlock<?, ANSWER, ?, ?>> rootReactiveBlock;
    private Throwable doneException;
    Queue<ANSWER> queue;
    boolean isPulling;
    boolean isInitialised;

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    protected ReasonerProducer(Options.Query options, Registry controllerRegistry,
                               ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = new AtomicBoolean(false);
        this.requiredAnswers = new AtomicInteger();
        this.isPulling = false;
        this.isInitialised = false;
    }

    protected Registry controllerRegistry() {
        return controllerRegistry;
    }

    @Override
    public synchronized void produce(Queue<ANSWER> queue, int requestedAnswers, Executor executor) {
        assert this.queue == null || this.queue == queue;
        assert requestedAnswers > 0;
        if (!done.get()) {
            this.queue = queue;
            requiredAnswers.addAndGet(requestedAnswers);
            if (!isInitialised) initialiseRoot();
            if (rootReactiveBlock != null && !isPulling) pull();
        } else {
            if (doneException == null) queue.done();
            else queue.done(doneException);
        }
    }

    protected abstract void initialiseRoot();

    @Override
    public void rootReactiveBlockFinalised(Actor.Driver<? extends AbstractReactiveBlock<?, ANSWER, ?, ?>> rootReactiveBlock) {
        assert this.rootReactiveBlock == null;
        this.rootReactiveBlock = rootReactiveBlock;
        if (requiredAnswers.get() > 0) pull();
    }

    protected void pull() {
        assert rootReactiveBlock != null;
        isPulling = true;
        rootReactiveBlock.execute(actor -> actor.rootPull());
    }

    @Override
    public void finish() {
        // note: root resolver calls this single-threaded, so is thread safe
        LOG.trace("All answers found.");
        if (!done.getAndSet(true)) {
            if (queue == null) {
                assert !isPulling;
                assert requiredAnswers.get() == 0;
            } else {
                requiredAnswers.set(0);
                queue.done();
            }
        }
    }

    @Override
    public void exception(Throwable e) {
        if (!done.getAndSet(true)) {
            doneException = e;
            if (queue == null) {
                assert !isPulling;
                assert requiredAnswers.get() == 0;
            } else {
                requiredAnswers.set(0);
                queue.done(e.getCause());
            }
        }
    }

    @Override
    public void recycle() {

    }

    public static abstract class Match extends ReasonerProducer<ConceptMap> {

        public Match(Options.Query options, Registry controllerRegistry, ExplainablesManager explainablesManager) {
            super(options, controllerRegistry, explainablesManager);
        }

        @Override
        public void receiveAnswer(ConceptMap answer) {
            isPulling = false;
            // TODO: The explainables can always be given. The only blocked to removing the explain flag is that
            //  `match get` filters should be ignored for an explainable answer
            if (options.explain() && !answer.explainables().isEmpty()) {
                explainablesManager.setAndRecordExplainables(answer);
            }
            queue.put(answer);
            if (requiredAnswers.decrementAndGet() > 0) pull();
        }

        public static class Conjunction extends Match {

            private final com.vaticle.typedb.core.pattern.Conjunction conjunction;
            private final Set<Identifier.Variable.Retrievable> filter;

            public Conjunction(com.vaticle.typedb.core.pattern.Conjunction conjunction,
                               Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                               Registry controllerRegistry, ExplainablesManager explainablesManager) {
                super(options, controllerRegistry, explainablesManager);
                this.conjunction = conjunction;
                this.filter = filter;
            }

            @Override
            protected void initialiseRoot() {
                controllerRegistry().registerRootConjunction(conjunction, filter, options.explain(), this);
                isInitialised = true;
            }
        }

        public static class Disjunction extends Match {

            private final com.vaticle.typedb.core.pattern.Disjunction disjunction;
            private final Set<Identifier.Variable.Retrievable> filter;

            public Disjunction(com.vaticle.typedb.core.pattern.Disjunction disjunction,
                               Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                               Registry controllerRegistry, ExplainablesManager explainablesManager) {
                super(options, controllerRegistry, explainablesManager);
                this.disjunction = disjunction;
                this.filter = filter;
            }

            @Override
            protected void initialiseRoot() {
                controllerRegistry().registerRootDisjunction(disjunction, filter, options.explain(), this);
                isInitialised = true;
            }
        }
    }

    public static class Explain extends ReasonerProducer<Explanation> {

        private final Concludable concludable;
        private final ConceptMap bounds;

        public Explain(Concludable concludable, ConceptMap bounds, Options.Query options, Registry controllerRegistry,
                       ExplainablesManager explainablesManager) {
            super(options, controllerRegistry, explainablesManager);
            this.concludable = concludable;
            this.bounds = bounds;
        }

        @Override
        protected void initialiseRoot() {
            controllerRegistry().registerExplainableRoot(concludable, bounds, this);
            isInitialised = true;
        }

        @Override
        public void receiveAnswer(Explanation explanation) {
            isPulling = false;
            if (!explanation.conditionAnswer().explainables().isEmpty()) {
                explainablesManager.setAndRecordExplainables(explanation.conditionAnswer());
            }
            queue.put(explanation);
            if (requiredAnswers.decrementAndGet() > 0) pull();
        }
    }
}
