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
 *
 */

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.poller.Poller;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver.DownstreamManager;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.poller.Pollers.poll;

public abstract class RequestState {

    protected final Request fromUpstream;
    private final int iteration;

    protected RequestState(Request fromUpstream, int iteration) {
        this.fromUpstream = fromUpstream;
        this.iteration = iteration;
    }

    public abstract Optional<? extends AnswerState.Partial<?>> nextAnswer();

    public int iteration() {
        return iteration;
    }

    public boolean isExploration() {
        return false;
    }

    public Exploration asExploration() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Exploration.class));
    }

    public interface Exploration {

        void newAnswer(AnswerState.Partial<?> partial);

        DownstreamManager downstreamManager();

        boolean singleAnswerRequired();

    }

    public abstract static class CachingRequestState<ANSWER, SUBSUMES> extends RequestState {

        protected final AnswerCache<ANSWER, SUBSUMES> answerCache;
        protected final boolean isSubscriber;
        protected Poller<? extends AnswerState.Partial<?>> cacheReader;
        protected final Set<ConceptMap> deduplicationSet;

        protected CachingRequestState(Request fromUpstream, AnswerCache<ANSWER, SUBSUMES> answerCache, int iteration,
                                      boolean deduplicate, boolean isSubscriber) {
            super(fromUpstream, iteration);
            this.answerCache = answerCache;
            this.isSubscriber = isSubscriber;
            this.deduplicationSet = deduplicate ? new HashSet<>() : null;
            this.cacheReader = answerCache.reader(isSubscriber).flatMap(
                    a -> poll(toUpstream(a).filter(
                            partial -> !deduplicate || !deduplicationSet.contains(partial.conceptMap()))));
        }

        @Override
        public Optional<? extends AnswerState.Partial<?>> nextAnswer() {
            Optional<? extends AnswerState.Partial<?>> ans = cacheReader.poll();
            if (ans.isPresent() && deduplicationSet != null) deduplicationSet.add(ans.get().conceptMap());
            return ans;
        }

        protected abstract FunctionalIterator<? extends AnswerState.Partial<?>> toUpstream(ANSWER answer);

        public AnswerCache<ANSWER, SUBSUMES> answerCache() {
            return answerCache;
        }

    }

}
