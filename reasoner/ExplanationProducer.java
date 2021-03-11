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
import grakn.core.concurrent.producer.Producer;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.answer.Explanation;
import grakn.core.reasoner.resolution.framework.ResolutionTracer;

import java.util.concurrent.ExecutorService;

public class ExplanationProducer implements Producer<Explanation> {

    private final Options.Query options;
    private final Conjunction conjunction;
    private final ConceptMap bounds;
//    private final Actor.Driver<Explainer> explainer;
//    private final Request explainRequest;
    private boolean requiresReiteration;
    private int iteration;
    private Queue<Explanation> queue;

    public ExplanationProducer(Conjunction conjunction, ConceptMap bounds, ResolverRegistry registry, Options.Query options) {
        this.conjunction = conjunction;
        this.bounds = bounds;
        this.options = options;
        this.queue = null;
        this.requiresReiteration = false;
        this.iteration = 0;
//        this.explainer = registry.explainer(conjunction, bounds, this::requestAnswered, this::requestFailed, this::exception);
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());

    }

    @Override
    public void produce(Queue<Explanation> queue, int toRequest, ExecutorService executor) {
        assert this.queue == null || this.queue == queue;
        this.queue = queue;
        for (int i = 0; i < toRequest; i++) {
            requestExplanation();
        }
    }

    private void requestExplanation() {
        if (options.traceInference()) ResolutionTracer.get().start();
//        explainer.execute(explainer -> explainer.receiveRequest(explainRequest, iteration));
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestAnswered(Top answerWithExplanation) {
        if (options.traceInference()) ResolutionTracer.get().finish();
//        if (answerWithExplanation.requiresReiteration()) requiresReiteration = true;
//
//        queue.put(answer.conceptMap());
//        if (required.decrementAndGet() > 0) requestAnswer();
//        else processing.decrementAndGet();
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestFailed(int iteration) {
//        LOG.trace("Failed to find answer to request in iteration: " + iteration);
//        if (options.traceInference()) ResolutionTracer.get().finish();
//        if (!done && iteration == this.iteration && !mustReiterate()) {
//            // query is completely terminated
//            done = true;
//            queue.done();
//            required.set(0);
//            return;
//        }
//
//        if (!done) {
//            if (iteration == this.iteration) prepareNextIteration();
//            assert iteration < this.iteration;
//            retryInNewIteration();
//        }
    }

    private void exception(Throwable e) {
//        if (!done) {
//            done = true;
//            required.set(0);
//            queue.done(e);
//        }
    }


    @Override
    public void recycle() {

    }
}
