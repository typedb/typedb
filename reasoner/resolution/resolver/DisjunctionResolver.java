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
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Compound;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
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
                                  com.vaticle.typedb.core.pattern.Disjunction disjunction, ControllerRegistry registry) {
        // TODO: This class takes a pattern disjunction whereas nested disjunctions take a core disjunction
        super(driver, name, registry);
        this.disjunction = disjunction;
        this.downstreamResolvers = new HashMap<>();
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream) {
        LOG.trace("{}: received answer: {}", name(), fromDownstream);
        if (isTerminated()) return;

        Request fromUpstream = upstreamRequest(fromDownstream);
        ResolutionState resolutionState = resolutionStates.get(fromUpstream.visit().partialAnswer().asCompound());

        assert fromDownstream.answer().isCompound();
        AnswerState answer = toUpstreamAnswer(fromDownstream.answer().asCompound(), fromDownstream);
        boolean acceptedAnswer = tryAcceptUpstreamAnswer(answer, fromUpstream);
        if (!acceptedAnswer) sendNextMessage(fromUpstream, resolutionState);
    }

    protected abstract boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream);

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
    protected ResolutionState createResolutionState(Compound<?, ?> fromUpstream) {
        LOG.debug("{}: Creating a new ResolutionState for request: {}", name(), fromUpstream);
        ResolutionState resolutionState = new ResolutionState();
        for (Driver<ConjunctionResolver.Nested> conjunctionResolver : downstreamResolvers.keySet()) {
            Compound.Nestable downstream = fromUpstream.filterToNestable(conjunctionRetrievedIds(conjunctionResolver));
            Request.Factory request = Request.Factory.create(driver(), conjunctionResolver, downstream);
            resolutionState.explorationManager().add(request);
        }
        return resolutionState;
    }

    private static Set<Identifier.Variable.Retrievable> conjunctionRetrievedIds(
            Driver<ConjunctionResolver.Nested> conjunctionResolver) {
        // TODO use a map from resolvable to resolvers, then we don't have to reach into the state and use the conjunction
        return iterate(conjunctionResolver.actor().conjunction().variables()).filter(v -> v.id().isRetrievable())
                .map(v -> v.id().asRetrievable()).toSet();
    }

    public static class Nested extends DisjunctionResolver<Nested> {

        public Nested(Driver<Nested> driver, Disjunction disjunction, ControllerRegistry registry) {
            super(driver, Nested.class.getSimpleName() + "(pattern: " + disjunction + ")", disjunction, registry);
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream) {
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
