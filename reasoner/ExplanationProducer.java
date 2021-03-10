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

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.producer.Producer;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.Explanation;
import grakn.core.reasoner.resolution.framework.ResolutionTracer;
import grakn.core.reasoner.resolution.resolver.Explainer;

import java.util.concurrent.ExecutorService;

public class ExplanationProducer implements Producer<Explanation> {

    private Conjunction conjunction;
    private ConceptMap bounds;

    public ExplanationProducer(Conjunction conjunction, ConceptMap bounds, ResolverRegistry registry) {
        this.conjunction = conjunction;
        this.bounds = bounds;
        Actor.Driver<Explainer> explainer = registry.explainer(conjunction, bounds, this::requestAnswered, this::requestFailed, this::exception);
    }

    @Override
    public void produce(Queue<Explanation> queue, int request, ExecutorService executor) {

    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestAnswered(Explanation explanation) {
//        if (options.traceInference()) ResolutionTracer.get().finish();
//        if (answer.requiresReiteration()) requiresReiteration = true;
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
