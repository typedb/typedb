/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.concurrent.actor.Actor;
import grakn.core.common.producer.Producer;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.NoOpAggregator;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;

public class ReasonerProducer implements Producer<ConceptMap> {

    private final Actor<RootResolver> rootResolver;
    private final ResolverRegistry registry;
    private Request resolveRequest;
    private boolean done;
    private Sink<ConceptMap> sink = null;
    private int iteration;
    private boolean iterationInferredAnswer;

    public ReasonerProducer(Conjunction conjunction, ResolverRegistry resolverRegistry) {
        this.rootResolver = resolverRegistry.createRoot(conjunction, this::requestAnswered, this::requestFailed);
        this.registry = resolverRegistry;
        this.iteration = 0;
        this.resolveRequest = new Request(new Request.Path(rootResolver), NoOpAggregator.create(), ResolutionAnswer.Derivation.EMPTY, iteration);
    }

    @Override
    public void produce(Sink<ConceptMap> sink, int count) {
        assert this.sink == null || this.sink == sink;
        this.sink = sink;
        for (int i = 0; i < count; i++) {
            requestAnswer();
        }
    }

    @Override
    public void recycle() {

    }

    private void requestAnswered(final ResolutionAnswer answer) {
        if (answer.isInferred()) iterationInferredAnswer = true;
        sink.put(answer.aggregated().conceptMap());
    }

    private void requestFailed(int iteration) {
        assert this.iteration == iteration || this.iteration == iteration + 1;

        if (iteration == this.iteration && !mustReiterate()) {
            if (!done) {
                done = true;
                sink.done(this);
            }
        } else if (this.iteration == iteration + 1) {
            retry();
        } else {
            nextIteration();
            retry();
        }
    }

    private void nextIteration() {
        iteration++;
        resolveRequest = new Request(new Request.Path(rootResolver), NoOpAggregator.create(), ResolutionAnswer.Derivation.EMPTY, iteration);
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
        rootResolver.tell(actor -> actor.executeReceiveRequest(resolveRequest, registry));
    }
}
