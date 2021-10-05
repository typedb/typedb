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

import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Match.Finished;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface RootResolver {

    class Conjunction extends ConjunctionResolver<Conjunction> {

        private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

        private final com.vaticle.typedb.core.pattern.Conjunction conjunction;
        private final BiConsumer<Request, Finished> onAnswer;
        private final Consumer<Request> onFail;
        private final Consumer<Throwable> onException;

        public Conjunction(Driver<Conjunction> driver, com.vaticle.typedb.core.pattern.Conjunction conjunction,
                           BiConsumer<Request, Finished> onAnswer, Consumer<Request> onFail, Consumer<Throwable> onException,
                           ResolverRegistry registry) {
            super(driver, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", registry);
            this.conjunction = conjunction;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
        }

        @Override
        public com.vaticle.typedb.core.pattern.Conjunction conjunction() {
            return conjunction;
        }

        @Override
        Set<Concludable> concludablesTriggeringRules() {
            return Iterators.iterate(Concludable.create(conjunction))
                    .filter(c -> c.getApplicableRules(registry.conceptManager(), registry.logicManager()).hasNext())
                    .toSet();
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream) {
            assert answer.isTop() && answer.asTop().isMatch() && answer.asTop().asMatch().isFinished();
            Finished finished = answer.asTop().asMatch().asFinished();
            LOG.debug("Submitting answer: {}", finished);
            onAnswer.accept(fromUpstream, finished);
        }

        @Override
        protected void failToUpstream(Request fromUpstream) {
            LOG.debug("Submitting fail for request: {}", fromUpstream);
            onFail.accept(fromUpstream);
        }

        @Override
        protected Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer) {
            assert partialAnswer.isRoot() && partialAnswer.isMatch();
            return Optional.of(partialAnswer.asRoot().asMatch().toFinishedTop(conjunction));
        }

        @Override
        ResolutionState resolutionStateNew() {
            return new ResolutionState();
        }

    }

    class Disjunction extends DisjunctionResolver<Disjunction> {

        private static final Logger LOG = LoggerFactory.getLogger(Disjunction.class);
        private final BiConsumer<Request, Finished> onAnswer;
        private final Consumer<Request> onFail;
        private final Consumer<Throwable> onException;

        public Disjunction(Driver<Disjunction> driver, com.vaticle.typedb.core.pattern.Disjunction disjunction,
                           BiConsumer<Request, Finished> onAnswer, Consumer<Request> onFail,
                           Consumer<Throwable> onException, ResolverRegistry registry) {
            super(driver, Disjunction.class.getSimpleName() + "(pattern:" + disjunction + ")", disjunction, registry);
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
            this.isInitialised = false;
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream) {
            assert answer.isTop() && answer.asTop().isMatch() && answer.asTop().asMatch().isFinished();
            Finished finished = answer.asTop().asMatch().asFinished();
            LOG.debug("Submitting answer: {}", finished);
            onAnswer.accept(fromUpstream, finished);
        }

        @Override
        protected void failToUpstream(Request fromUpstream) {
            onFail.accept(fromUpstream);
        }

        @Override
        protected boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream) {
            ResolutionState resolutionState = resolutionStates.get(fromUpstream.visit().factory());
            if (!resolutionState.deduplicationSet().contains(upstreamAnswer.conceptMap())) {
                resolutionState.deduplicationSet().add(upstreamAnswer.conceptMap());
                answerToUpstream(upstreamAnswer, fromUpstream);
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected AnswerState toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer, Response.Answer fromDownstream) {
            assert partialAnswer.isRoot() && partialAnswer.isMatch();
            Driver<? extends Resolver<?>> sender = fromDownstream.sourceRequest().receiver();
            com.vaticle.typedb.core.pattern.Conjunction patternAnswered = downstreamResolvers.get(sender);
            return partialAnswer.asRoot().asMatch().toFinishedTop(patternAnswered);
        }

    }

    class Explain extends ConjunctionResolver<Explain> {

        private static final Logger LOG = LoggerFactory.getLogger(Explain.class);

        private final com.vaticle.typedb.core.pattern.Conjunction conjunction;
        private final BiConsumer<Request, Top.Explain.Finished> onAnswer;
        private final Consumer<Request> onFail;
        private final Consumer<Throwable> onException;

        private final Set<Explanation> submittedExplanations;

        public Explain(Driver<Explain> driver, com.vaticle.typedb.core.pattern.Conjunction conjunction,
                       BiConsumer<Request, Top.Explain.Finished> onAnswer,
                       Consumer<Request> onFail,
                       Consumer<Throwable> onException, ResolverRegistry registry) {
            super(driver, "Explain(" + conjunction + ")", registry);
            this.conjunction = conjunction;
            this.onAnswer = onAnswer;
            this.onFail = onFail;
            this.onException = onException;
            this.submittedExplanations = new HashSet<>();
        }

        @Override
        public void terminate(Throwable cause) {
            super.terminate(cause);
            onException.accept(cause);
        }

        @Override
        protected void answerToUpstream(AnswerState answer, Request fromUpstream) {
            assert answer.isTop() && answer.asTop().isExplain() && answer.asTop().asExplain().isFinished();
            Top.Explain.Finished finished = answer.asTop().asExplain().asFinished();
            LOG.debug("Submitting answer: {}", finished);
            onAnswer.accept(fromUpstream, finished);
        }

        @Override
        protected void failToUpstream(Request fromUpstream) {
            onFail.accept(fromUpstream);
        }

        @Override
        Optional<AnswerState> toUpstreamAnswer(Partial.Compound<?, ?> partialAnswer) {
            assert partialAnswer.isRoot() && partialAnswer.isExplain();
            return Optional.of(partialAnswer.asRoot().asExplain().toFinishedTop());
        }

        @Override
        boolean tryAcceptUpstreamAnswer(AnswerState upstreamAnswer, Request fromUpstream) {
            assert upstreamAnswer.isTop() && upstreamAnswer.asTop().isExplain() && upstreamAnswer.asTop().asExplain().isFinished();
            Top.Explain.Finished finished = upstreamAnswer.asTop().asExplain().asFinished();
            if (!submittedExplanations.contains(finished.explanation())) {
                submittedExplanations.add(finished.explanation());
                answerToUpstream(upstreamAnswer, fromUpstream);
                return true;
            } else {
                return false;
            }
        }

        @Override
        ResolutionState resolutionStateNew() {
            return new ResolutionState();
        }

        @Override
        Set<Concludable> concludablesTriggeringRules() {
            Set<Concludable> concludables = Iterators.iterate(Concludable.create(conjunction))
                    .filter(c -> c.getApplicableRules(registry.conceptManager(), registry.logicManager()).hasNext())
                    .toSet();
            assert concludables.size() == 1;
            return concludables;
        }

        @Override
        com.vaticle.typedb.core.pattern.Conjunction conjunction() {
            return conjunction;
        }
    }

}
