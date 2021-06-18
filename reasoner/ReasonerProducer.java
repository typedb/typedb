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
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound.Root;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Match.Finished;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerStateImpl.TopImpl.MatchImpl.InitialImpl;
import com.vaticle.typedb.core.reasoner.resolution.framework.ReiterationQuery;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

@ThreadSafe
public class ReasonerProducer implements Producer<ConceptMap> {

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final Actor.Driver<? extends Resolver<?>> rootResolver;
    private final AtomicInteger required;
    private final AtomicInteger processing;
    private final Options.Query options;
    private final ResolverRegistry resolverRegistry;
    private final ExplainablesManager explainablesManager;
    private final Request resolveRequest;
    private final ReiterationQuery.Request reiterationRequest;
    private final int computeSize;
    private boolean done;
    private int iteration;
    private Queue<ConceptMap> queue;
    private Set<Actor.Driver<? extends Resolver<?>>> reiterationQueryRespondents;
    private boolean requiresReiteration;
    private boolean sentReiterationRequests;

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    public ReasonerProducer(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            ResolverRegistry resolverRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.resolverRegistry = resolverRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.iteration = 0;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.rootResolver = this.resolverRegistry.root(conjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        Root<?, ?> downstream = InitialImpl.create(filter, new ConceptMap(), this.rootResolver, options.explain()).toDownstream();
        this.resolveRequest = Request.create(rootResolver, downstream);
        this.reiterationRequest = ReiterationQuery.Request.create(rootResolver, this::receiveReiterationResponse);
        this.sentReiterationRequests = false;
        this.requiresReiteration = false;
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    public ReasonerProducer(Disjunction disjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            ResolverRegistry resolverRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.resolverRegistry = resolverRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.iteration = 0;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.rootResolver = this.resolverRegistry.root(disjunction, this::requestAnswered, this::requestFailed, this::exception);
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        Root<?, ?> downstream = InitialImpl.create(filter, new ConceptMap(), this.rootResolver, options.explain()).toDownstream();
        this.resolveRequest = Request.create(rootResolver, downstream);
        this.reiterationRequest = ReiterationQuery.Request.create(rootResolver, this::receiveReiterationResponse);
        this.sentReiterationRequests = false;
        this.requiresReiteration = false;
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
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
    private void requestAnswered(Finished answer) {
        if (options.traceInference()) ResolutionTracer.get().finish();
        ConceptMap conceptMap = answer.conceptMap();
        if (options.explain() && !conceptMap.explainables().isEmpty()) {
            explainablesManager.setAndRecordExplainables(conceptMap);
        }
        queue.put(conceptMap);
        if (required.decrementAndGet() > 0) requestAnswer();
        else processing.decrementAndGet();
    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestFailed(int iteration) {
        LOG.trace("Failed to find answer to request in iteration: " + iteration);
        if (options.traceInference()) ResolutionTracer.get().finish();
        if (resolverRegistry.concludableResolvers().size() == 0) {
            finish();
        } else if (!sentReiterationRequests && iteration == this.iteration) {
            sendReiterationRequests();
        }
    }

    private void finish() {
        // query is completely terminated
        done = true;
        queue.done();
        required.set(0);
    }

    private void sendReiterationRequests() {
        assert reiterationQueryRespondents == null || reiterationQueryRespondents.isEmpty();
        sentReiterationRequests = true;
        reiterationQueryRespondents = new HashSet<>(resolverRegistry.concludableResolvers());
        resolverRegistry.concludableResolvers()
                .forEach(res -> res.execute(actor -> actor.receiveReiterationQuery(reiterationRequest)));
    }

    private synchronized void receiveReiterationResponse(ReiterationQuery.Response response) {
        if (response.reiterate()) requiresReiteration = true;
        assert reiterationQueryRespondents.contains(response.sender());
        reiterationQueryRespondents.remove(response.sender());

        if (reiterationQueryRespondents.isEmpty()) {
            if (requiresReiteration) {
                prepareNextIteration();
                retryInNewIteration();
            } else {
                finish();
            }
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
        sentReiterationRequests = false;
        requiresReiteration = false;
    }

    private void retryInNewIteration() {
        requestAnswer();
    }

    private void requestAnswer() {
        if (options.traceInference()) ResolutionTracer.get().start();
        rootResolver.execute(actor -> actor.receiveRequest(resolveRequest, iteration));
    }
}
