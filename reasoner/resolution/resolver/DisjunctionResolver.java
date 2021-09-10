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

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound;
import com.vaticle.typedb.core.reasoner.resolution.framework.RequestFactory;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Traced;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class DisjunctionResolver<RESOLVER extends DisjunctionResolver<RESOLVER>> extends CompoundResolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);

    final Map<Driver<ConjunctionResolver.Nested>, com.vaticle.typedb.core.pattern.Conjunction> downstreamResolvers;
    final com.vaticle.typedb.core.pattern.Disjunction disjunction;

    protected DisjunctionResolver(Driver<RESOLVER> driver, String name,
                                  com.vaticle.typedb.core.pattern.Disjunction disjunction, ResolverRegistry registry) {
        // TODO: This class takes a pattern disjunction whereas nested disjunctions take a core disjunction
        super(driver, name, registry);
        this.disjunction = disjunction;
        this.downstreamResolvers = new HashMap<>();
    }

    @Override
    protected void receiveAnswer(Traced<Response.Answer> fromDownstream) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Traced<Request> fromUpstream = upstreamTracedRequest(fromDownstream);
        RequestState requestState = requestStates.get(fromUpstream.message().visit());

        assert fromDownstream.message().answer().isCompound();
        AnswerState answer = toUpstreamAnswer(fromDownstream.message().answer().asCompound(), fromDownstream.message());
        boolean acceptedAnswer = tryAcceptUpstreamAnswer(answer, fromUpstream);
        if (!acceptedAnswer) nextAnswer(fromUpstream, requestState);
    }

    protected abstract boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Traced<Request> fromUpstream);

    protected abstract AnswerState toUpstreamAnswer(Compound<?, ?> answer, Response.Answer fromDownstream);

    @Override
    protected void initialiseDownstreamResolvers() {
        LOG.debug("{}: initialising downstream resolvers", name());
        for (com.vaticle.typedb.core.pattern.Conjunction conjunction : disjunction.conjunctions()) {
            try {
                downstreamResolvers.put(registry.nested(conjunction), conjunction);
            } catch (TypeDBException e) {
                terminate(e);
                return;
            }
        }
        if (!isTerminated()) isInitialised = true;
    }

    @Override
    protected RequestState requestStateCreate(Request.Visit fromUpstream) {
        LOG.debug("{}: Creating a new RequestState for request: {}", name(), fromUpstream);
        assert fromUpstream.partialAnswer().isCompound();
        RequestState requestState = new RequestState();
        for (Driver<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers.keySet()) {
            Compound.Nestable downstream = fromUpstream.partialAnswer().asCompound()
                    .filterToNestable(conjunctionRetrievedIds(conjunctionResolver));
            RequestFactory request = RequestFactory.create(driver(), conjunctionResolver, downstream);
            requestState.downstreamManager().add(request);
        }
        return requestState;
    }

    private static Set<Identifier.Variable.Retrievable> conjunctionRetrievedIds(
            Driver<ConjunctionResolver.Nested> conjunctionResolver) {
        // TODO use a map from resolvable to resolvers, then we don't have to reach into the state and use the conjunction
        return iterate(conjunctionResolver.actor().conjunction().variables()).filter(v -> v.id().isRetrievable())
                .map(v -> v.id().asRetrievable()).toSet();
    }

    public static class Nested extends DisjunctionResolver<Nested> {

        public Nested(Driver<Nested> driver, Disjunction disjunction, ResolverRegistry registry) {
            super(driver, Nested.class.getSimpleName() + "(pattern: " + disjunction + ")", disjunction, registry);
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Traced<Request> fromUpstream) {
            answerToUpstream(upstreamAnswer, fromUpstream);
            return true;
        }

        @Override
        protected AnswerState toUpstreamAnswer(Compound<?, ?> answer, Response.Answer fromDownstream) {
            assert answer.isNestable();
            return answer.asNestable().toUpstream();
        }

    }
}
