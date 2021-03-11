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
import grakn.core.concurrent.executor.Executors;
import grakn.core.concurrent.producer.Producer;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Identity;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionTracer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.UnboundVariable;
import graql.lang.query.GraqlMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static grakn.core.common.iterator.Iterators.iterate;

@ThreadSafe
public class ReasonerProducer implements Producer<ConceptMap> {

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final Actor.Driver<? extends Resolver<?>> rootResolver;
    private final AtomicInteger required;
    private final AtomicInteger processing;
    private final Options.Query options;
    private final Request resolveRequest;
    private final boolean recordExplanations = false; // TODO: make settable
    private final int computeSize;
    private boolean requiresReiteration;
    private boolean done;
    private int iteration;
    private Queue<ConceptMap> queue;

    // TODO: this class should be be a Producer implement a different async processing mechanism
    public ReasonerProducer(Conjunction conjunction, ResolverRegistry resolverRegistry, GraqlMatch.Modifiers modifiers,
                            Options.Query options) {
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
        this.rootResolver = resolverRegistry.root(conjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.options = options;
        Identity downstream = Top.initial(filter(modifiers.filter()), recordExplanations, this.rootResolver).toDownstream();
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        this.resolveRequest = Request.create(rootResolver, downstream);
        this.queue = null;
        this.iteration = 0;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
    }

    public ReasonerProducer(Disjunction disjunction, ResolverRegistry resolverRegistry, GraqlMatch.Modifiers modifiers,
                            Options.Query options) {
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
        this.rootResolver = resolverRegistry.root(disjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.options = options;
        Identity downstream = Top.initial(filter(modifiers.filter()), recordExplanations, this.rootResolver).toDownstream();
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        this.resolveRequest = Request.create(rootResolver, downstream);
        this.queue = null;
        this.iteration = 0;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
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
    public void recycle() {}

    private Set<Identifier.Variable.Name> filter(List<UnboundVariable> filter) {
        return iterate(filter).map(v -> Identifier.Variable.of(v.reference().asName())).toSet();
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestAnswered(Top resolutionAnswer) {
        if (options.traceInference()) ResolutionTracer.get().finish();
        if (resolutionAnswer.requiresReiteration()) requiresReiteration = true;
        queue.put(resolutionAnswer.conceptMap());
        if (required.decrementAndGet() > 0) requestAnswer();
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

    private void exception(Throwable e) {
        if (!done) {
            done = true;
            required.set(0);
            queue.done(e);
        }
    }

    private void prepareNextIteration() {
        iteration++;
        requiresReiteration = false;
    }

    private boolean mustReiterate() {
        /*
        TODO: room for optimisation:
        for example, reiteration should never be required if there
        are no loops in the rule graph
        NOTE: double check this logic holds in the actor execution model, eg. because of asynchrony, we may
        always have to reiterate until no more answers are found.

        counter example: $x isa $type; -> unifies with then { (friend: $y) isa friendship; }
        Without reiteration we will miss $x = instance, $type = relation/thing
         */
        return requiresReiteration;
    }

    private void retryInNewIteration() {
        requestAnswer();
    }

    private void requestAnswer() {
        if (options.traceInference()) ResolutionTracer.get().start();
        rootResolver.execute(actor -> actor.receiveRequest(resolveRequest, iteration));
    }
}
