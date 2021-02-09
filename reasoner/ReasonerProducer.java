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
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.UpstreamVars.Initial;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static grakn.core.reasoner.resolution.framework.ResolutionAnswer.Derivation.EMPTY;

@ThreadSafe
public class ReasonerProducer implements Producer<ConceptMap> {
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final Actor<? extends Resolver<?>> rootResolver;
    private final Set<Reference.Name> filter;
    private Queue<ConceptMap> queue;
    private Request resolveRequest;
    private boolean iterationInferredAnswer;
    private boolean done;
    private int iteration;

    public ReasonerProducer(Conjunction conjunction, ResolverRegistry resolverRegistry, Set<Reference.Name> filter) {
        this.rootResolver = resolverRegistry.rootConjunction(conjunction, this::requestAnswered, this::requestFailed);
        this.filter = filter;
        AnswerState.DownstreamVars.Identity downstream = Initial.of(new ConceptMap()).toDownstreamVars();
        this.resolveRequest = Request.create(new Request.Path(rootResolver, downstream), downstream, EMPTY, this.filter);
        this.queue = null;
        this.iteration = 0;
        this.done = false;
    }

    public ReasonerProducer(Disjunction disjunction, ResolverRegistry resolverRegistry, Set<Reference.Name> filter) {
        this.rootResolver = resolverRegistry.rootDisjunction(disjunction, this::requestAnswered, this::requestFailed);
        this.filter = filter;
        AnswerState.DownstreamVars.Identity downstream = Initial.of(new ConceptMap()).toDownstreamVars();
        this.resolveRequest = Request.create(new Request.Path(rootResolver, downstream), downstream , EMPTY, this.filter);
        this.queue = null;
        this.iteration = 0;
        this.done = false;
    }

    @Override
    public void produce(Queue<ConceptMap> queue, int request, ExecutorService executor) {
        assert this.queue == null || this.queue == queue;
        this.queue = queue;
        for (int i = 0; i < request; i++) {
            requestAnswer();
        }
    }

    @Override
    public void recycle() {}

    private void requestAnswered(ResolutionAnswer resolutionAnswer) {
        if (resolutionAnswer.isInferred()) iterationInferredAnswer = true;
        queue.put(resolutionAnswer.derived().withInitialFiltered());
    }

    private void requestFailed(int iteration) {
        LOG.trace("Failed to find answer to request in iteration: " + iteration);

        if (!done && iteration == this.iteration && !mustReiterate()) {
            // query is completely terminated
            done = true;
            queue.done();
            return;
        }

        if (!done) {
            if (iteration == this.iteration) {
                prepareNextIteration();
            }
            assert iteration < this.iteration;
            retryInNewIteration();
        }
    }

    private void prepareNextIteration() {
        iteration++;
        iterationInferredAnswer = false;
    }

    private boolean mustReiterate() {
        return iteration < 5;
        /*
        TODO room for optimisation:
        for example, reiteration should never be required if there
        are no loops in the rule graph
        NOTE: double check this logic holds in the actor execution model, eg. because of asynchrony, we may
        always have to reiterate until no more answers are found.
         */
//        return iterationInferredAnswer;
    }

    private void retryInNewIteration() {
        requestAnswer();
    }

    private void requestAnswer() {
        rootResolver.tell(actor -> actor.receiveRequest(resolveRequest, iteration));
    }
}
