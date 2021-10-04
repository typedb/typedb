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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class BoundRetrievableResolver extends Resolver<BoundRetrievableResolver> {

    private final AnswerCache<ConceptMap> cache;
    private final Map<Request.Template, RequestState> requestStates;
    private final ConceptMap bounds;

    public BoundRetrievableResolver(Driver<BoundRetrievableResolver> driver, Retrievable retrievable, ConceptMap bounds,
                                    ResolverRegistry registry) {
        super(driver, BoundRetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable.pattern() +
                " bounds: " + bounds.toString() + ")", registry);
        this.bounds = bounds;
        this.cache = new AnswerCache<>(() -> traversalIterator(retrievable.pattern(), bounds));
        this.requestStates = new HashMap<>();
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        assert fromUpstream.partialAnswer().conceptMap().equals(bounds);
        sendNextMessage(fromUpstream, requestStates.computeIfAbsent(
                fromUpstream.template(), request -> new BoundRequestState(request, cache)));
    }

    @Override
    protected void receiveRevisit(Request.Revisit fromUpstream) {
        assert fromUpstream.visit().partialAnswer().conceptMap().equals(bounds);
        receiveVisit(fromUpstream.visit());
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private void sendNextMessage(Request fromUpstream, RequestState requestState) {
        Optional<? extends AnswerState.Partial<?>> upstreamAnswer = requestState.nextAnswer();
        if (upstreamAnswer.isPresent()) {
            answerToUpstream(upstreamAnswer.get(), fromUpstream);
        } else {
            failToUpstream(fromUpstream);
        }
    }

    @Override
    protected void initialiseDownstreamResolvers() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private static class BoundRequestState extends CachingRequestState<ConceptMap> {

        public BoundRequestState(Request.Template fromUpstream, AnswerCache<ConceptMap> answerCache) {
            super(fromUpstream, answerCache, false);
        }

        @Override
        protected FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ConceptMap answer) {
            return Iterators.single(fromUpstream.partialAnswer().asRetrievable().aggregateToUpstream(answer));
        }
    }
}
