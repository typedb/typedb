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
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound.Root;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Explain;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerStateImpl;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import com.vaticle.typedb.core.reasoner.resolution.resolver.RootResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExplanationProducer implements Producer<Explanation> {

    private static final Logger LOG = LoggerFactory.getLogger(ExplanationProducer.class);

    private final ConceptMap bounds;
    private final ExplainablesManager explainablesManager;
    private final Options.Query options;
    private final Actor.Driver<RootResolver.Explain> explainer;
    private final int computeSize;
    private final AtomicInteger required;
    private final AtomicInteger processing;
    private int iteration;
    private boolean requiresReiteration;
    private boolean done;
    private int requestTraceIdCounter;
    private Queue<Explanation> queue;

    public ExplanationProducer(Conjunction conjunction, ConceptMap bounds, Options.Query options,
                               ResolverRegistry registry, ExplainablesManager explainablesManager) {
        this.bounds = bounds;
        this.explainablesManager = explainablesManager;
        this.options = options;
        this.queue = null;
        this.iteration = 0;
        this.requiresReiteration = false;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR : 1;
        this.explainer = registry.explainer(conjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.requestTraceIdCounter = 0;

        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    @Override
    public synchronized void produce(Queue<Explanation> queue, int request, Executor executor) {
        assert this.queue == null || this.queue == queue;
        this.queue = queue;
        this.required.addAndGet(request);
        int canRequest = computeSize - processing.get();
        int toRequest = Math.min(canRequest, request);
        for (int i = 0; i < toRequest; i++) {
            requestExplanation();
        }
        processing.addAndGet(toRequest);
    }

    private Request createExplanationRequest(int explainRequestId) {
        Root.Explain downstream = new AnswerStateImpl.TopImpl.ExplainImpl.InitialImpl(bounds, explainer).toDownstream();
        return Request.create(explainer, ResolutionTracer.TraceId.create(System.identityHashCode(this), explainRequestId), downstream);
    }

    private void requestExplanation() {
        Request explainRequest = createExplanationRequest(requestTraceIdCounter);
        if (options.traceInference()) ResolutionTracer.get().start(explainRequest);
        explainer.execute(explainer -> explainer.receiveRequest(explainRequest, iteration));
        requestTraceIdCounter += 1;
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestAnswered(Request requestAnswered, Explain.Finished explainedAnswer) {
        if (options.traceInference()) ResolutionTracer.get().finish(requestAnswered);
        Explanation explanation = explainedAnswer.explanation();
        explainablesManager.setAndRecordExplainables(explanation.conditionAnswer());
        queue.put(explanation);
        if (required.decrementAndGet() > 0) requestExplanation();
        else processing.decrementAndGet();
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestFailed(Request failedRequest, int iteration) {
        LOG.trace("Failed to find answer to request in iteration: " + iteration);
        if (options.traceInference()) ResolutionTracer.get().finish(failedRequest);
        if (!done && iteration == this.iteration && !mustReiterate()) {
            // query is completely terminated
            done = true;
            queue.done();
            required.set(0);
            return;
        }

        if (!done) {
            if (iteration == this.iteration) prepareNextIteration();
            assert iteration < this.iteration;
            retryInNewIteration();
        }
    }

    private boolean mustReiterate() {
        return requiresReiteration;
    }

    private void prepareNextIteration() {
        iteration++;
        requiresReiteration = false;
    }

    private void retryInNewIteration() {
        requestExplanation();
    }

    private void exception(Throwable e) {
        if (options.traceInference()) ResolutionTracer.get().exception();
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
