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
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import static grakn.core.reasoner.resolution.answer.AnswerState.DownstreamVars.Root;
import static grakn.core.reasoner.resolution.framework.ResolutionAnswer.Derivation.EMPTY;

@ThreadSafe // TODO: verify
public class ReasonerProducer implements Producer<ConceptMap> {
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final Actor<RootResolver> rootResolver;
    private Queue<ConceptMap> queue;
    private Request resolveRequest;
    private boolean iterationInferredAnswer;
    private boolean done;
    private int iteration;

    public ReasonerProducer(Conjunction conjunction, ResolverRegistry resolverMgr) {
        this.rootResolver = resolverMgr.createRoot(conjunction, this::requestAnswered, this::requestFailed);
        this.resolveRequest = Request.create(new Request.Path(rootResolver), Root.create(), EMPTY);
        this.queue = null;
        this.iteration = 0;
    }

    @Override
    public void produce(Queue<ConceptMap> queue, int request) {
        assert this.queue == null || this.queue == queue;
        this.queue = queue;
        for (int i = 0; i < request; i++) {
            requestAnswer();
        }
    }

    @Override
    public void recycle() {}

    private void requestAnswered(ResolutionAnswer answer) {
        if (answer.isInferred()) iterationInferredAnswer = true;
        queue.put(answer.derived().withInitial());
    }

    private void requestFailed(int iteration) {
        LOG.trace("New iteration {}", iteration);
        assert this.iteration == iteration || this.iteration == iteration + 1;

        if (iteration == this.iteration && mustReiterate()) {
            nextIteration();
            retry();
        } else if (this.iteration == iteration) {
            // fully terminated finding answers
            if (!done) {
                done = true;
                queue.done();
            }
        } else {
            // straggler request failing from prior iteration
            retry();
        }
    }

    private void nextIteration() {
        iteration++;
        iterationInferredAnswer = false;
        resolveRequest = Request.create(new Request.Path(rootResolver), Root.create(), EMPTY);
    }

    private boolean mustReiterate() {
        /*
        TODO room for optimisation:
        for example, reiteration should never be required if there
        are no loops in the rule graph
        NOTE: double check this logic holds in the actor execution model, eg. because of asynchrony, we may
        always have to reiterate until no more answers are found.
         */
        return iterationInferredAnswer;
    }

    private void retry() {
        requestAnswer();
    }

    private void requestAnswer() {
        rootResolver.tell(actor -> actor.receiveRequest(resolveRequest, iteration));
    }
}
