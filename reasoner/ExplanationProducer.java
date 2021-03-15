/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner;

import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.common.Executors;
import grakn.core.concurrent.producer.Producer;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Compound.ExplainRoot;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.answer.Explanation;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionTracer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ExplanationProducer implements Producer<Explanation> {

    private static final Logger LOG = LoggerFactory.getLogger(ExplanationProducer.class);

    private final Explanations explanations;
    private final Options.Query options;
    private final Actor.Driver<RootResolver.Explain> explainer;
    private final Request explainRequest;
    private final int computeSize;
    private final AtomicInteger required;
    private final AtomicInteger processing;
    private int iteration;
    private boolean requiresReiteration;
    private boolean done;

    private Queue<Explanation> queue;

    public ExplanationProducer(Conjunction conjunction, ConceptMap bounds, Options.Query options,
                               ResolverRegistry registry, Explanations explanations) {
        this.explanations = explanations;
        this.options = options;
        this.queue = null;
        this.iteration = 0;
        this.requiresReiteration = false;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        this.explainer = registry.explainer(conjunction, this::requestAnswered, this::requestFailed, this::exception);
        ExplainRoot downstream = Top.Explain.initial(bounds, explainer).toDownstream();
        this.explainRequest = Request.create(explainer, downstream);
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    @Override
    public synchronized void produce(Queue<Explanation> queue, int request, ExecutorService executor) {
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

    private void requestExplanation() {
        if (options.traceInference()) ResolutionTracer.get().start();
        explainer.execute(explainer -> explainer.receiveRequest(explainRequest, iteration));
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestAnswered(Top.Explain.Finished explainedAnswer) {
        if (options.traceInference()) ResolutionTracer.get().finish();
        if (explainedAnswer.requiresReiteration()) requiresReiteration = true;
        Explanation explanation = explainedAnswer.explanation();
        explanations.setAndRecordExplainableIds(explanation.conditionAnswer());
        queue.put(explanation);
        if (required.decrementAndGet() > 0) requestExplanation();
        else processing.decrementAndGet();
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestFailed(int iteration) {
        LOG.trace("Failed to find answer to request in iteration: " + iteration);
        if (options.traceInference()) ResolutionTracer.get().finish();
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
        return iteration < 10;

//        return false;
//        /*
//        TODO do we definitely have to reiterate when calculating explanations...?
//         */
//        return requiresReiteration;
    }

    private void prepareNextIteration() {
        iteration++;
        requiresReiteration = false;
    }

    private void retryInNewIteration() {
        requestExplanation();
    }

    private void exception(Throwable e) {
        if (options.traceInference()) ResolutionTracer.get().finish();
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
