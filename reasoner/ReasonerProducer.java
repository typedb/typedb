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
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound.Root;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Match.Finished;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerStateImpl.TopImpl.MatchImpl.InitialImpl;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class ReasonerProducer implements Producer<ConceptMap> { // TODO: Rename to MatchProducer and create abstract supertype

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final Actor.Driver<? extends Resolver<?>> rootResolver;
    private final AtomicInteger required;
    private final AtomicInteger processing;
    private final Options.Query options;
    private final ControllerRegistry controllerRegistry;
    private final ExplainablesManager explainablesManager;
    private final int computeSize;
    private final Request.Factory requestFactory;
    private final Request.Visit untracedResolveRequest;
    private final UUID traceId;
    private boolean done;
    private Queue<ConceptMap> queue;
    private int requestIdCounter;

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    public ReasonerProducer(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            ControllerRegistry controllerRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.rootResolver = this.controllerRegistry.root(conjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        this.requestIdCounter = 0;
        this.traceId = UUID.randomUUID();
        this.requestFactory = requestFactory(filter);
        this.untracedResolveRequest = requestFactory.createVisit();
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    public ReasonerProducer(Disjunction disjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            ControllerRegistry controllerRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.rootResolver = this.controllerRegistry.root(disjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        this.requestIdCounter = 0;
        this.traceId = UUID.randomUUID();
        this.requestFactory = requestFactory(filter);
        this.untracedResolveRequest = requestFactory.createVisit();
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    private Request.Factory requestFactory(Set<Identifier.Variable.Retrievable> filter) {
        Root<?, ?> downstream = InitialImpl.create(filter, new ConceptMap(), this.rootResolver, options.explain()).toDownstream();
        return Request.Factory.create(rootResolver, downstream);
    }

    @Override
    public synchronized void produce(Queue<ConceptMap> queue, int request, Executor executor) {
        assert this.queue == null || this.queue == queue;
        this.queue = queue;
        this.required.addAndGet(request);
        int canRequest = computeSize - processing.get();
        int toRequest = Math.min(canRequest, request);
        for (int i = 0; i < toRequest; i++) {
            requestAnswer();
        }
        processing.addAndGet(toRequest);
    }

    @Override
    public void recycle() {

    }

    // note: root resolver calls this single-threaded, so is thread safe
    private void requestAnswered(Request answeredRequest, Finished answer) {
        if (options.traceInference()) ResolutionTracer.get().finish(answeredRequest);
        ConceptMap conceptMap = answer.conceptMap();
        if (options.explain() && !conceptMap.explainables().isEmpty()) {
            explainablesManager.setAndRecordExplainables(conceptMap);
        }
        queue.put(conceptMap);
        if (required.decrementAndGet() > 0) requestAnswer();
        else processing.decrementAndGet();
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestFailed(Request failedRequest) {
        LOG.trace("Failed to find answer to request {}", failedRequest);
        if (options.traceInference()) ResolutionTracer.get().finish(failedRequest);
        finish();
    }

    private void finish() {
        if (!done) {
            // query is completely terminated
            done = true;
            queue.done();
            required.set(0);
        }
    }

    private void exception(Throwable e) {
        if (!done) {
            done = true;
            required.set(0);
            queue.done(e);
        }
    }

    private void requestAnswer() {
        Request.Visit visitRequest;
        if (options.traceInference()) {
            Trace trace = Trace.create(traceId, requestIdCounter);
            visitRequest = requestFactory.createVisit(trace);
            ResolutionTracer.get().start(visitRequest);
            requestIdCounter += 1;
        } else {
            visitRequest = untracedResolveRequest;
        }
        rootResolver.execute(actor -> actor.receiveVisit(visitRequest));
    }
}
