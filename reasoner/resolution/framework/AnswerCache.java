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
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.poller.AbstractPoller;
import com.vaticle.typedb.core.common.poller.Poller;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache.SubsumptionAnswerCache.ConceptMapCache;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class AnswerCache<ANSWER, SUBSUMES> {

    protected final List<ANSWER> answers;
    private final Set<ANSWER> answersSet;
    protected boolean reiterateOnAnswerAdded;
    protected boolean requiresReiteration;
    protected FunctionalIterator<ANSWER> answerSource;
    protected boolean complete;
    protected final Map<SUBSUMES, ? extends AnswerCache<?, SUBSUMES>> cacheRegister;
    protected final ConceptMap state;
    private final Supplier<FunctionalIterator<ANSWER>> answerSourceSupplier;
    private boolean sourceCleared;
    private boolean sourceExhausted;

    protected AnswerCache(Map<SUBSUMES, ? extends AnswerCache<?, SUBSUMES>> cacheRegister, ConceptMap state,
                          Supplier<FunctionalIterator<ANSWER>> answerSourceSupplier) {
        this.cacheRegister = cacheRegister;
        this.state = state;
        this.answerSourceSupplier = answerSourceSupplier;
        this.answerSource = null;
        this.answers = new ArrayList<>(); // TODO: Replace answer list and deduplication set with a bloom filter
        this.answersSet = new HashSet<>();
        this.reiterateOnAnswerAdded = false;
        this.requiresReiteration = false;
        this.complete = false;
        this.sourceCleared = false;
        this.sourceExhausted = false;
    }

    public void add(ANSWER answer) {
        assert !isComplete();
        if (addIfAbsent(answer) && reiterateOnAnswerAdded) requiresReiteration = true;
    }

    public void clearSource() {
        if (answerSource != null) answerSource.recycle();
        answerSource = empty();
        sourceCleared = true;
    }

    public Poller<ANSWER> reader(boolean isSubscriber) {
        return new Reader(isSubscriber);
    }

    public void setComplete() {
        complete = true;
        setSourceExhausted();
    }

    public boolean isComplete() {
        return complete;
    }

    public void setSourceExhausted() {
        sourceExhausted = true;
        if (answerSource != null) answerSource.recycle();
    }

    public boolean sourceExhausted() {
        return sourceExhausted;
    }

    public boolean requiresReiteration() {
        return requiresReiteration;
    }

    public boolean isConceptMapCache() {
        return false;
    }

    public ConceptMapCache asConceptMapCache() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), ConceptMapCache.class);
    }

    public boolean isConcludableAnswerCache() {
        return false;
    }

    public ConcludableAnswerCache asConcludableAnswerCache() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), ConcludableAnswerCache.class);
    }

    protected Optional<ANSWER> get(int index, boolean isSubscriber) {
        assert index >= 0;
        if (index < answers.size()) {
            return Optional.of(answers.get(index));
        } else if (index == answers.size()) {
            if (isComplete()) return Optional.empty();
            Optional<ANSWER> nextAnswer = searchForAnswer(index, isSubscriber);
            if (nextAnswer.isEmpty() && isSubscriber) reiterateOnAnswerAdded = true;
            return nextAnswer;
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    protected Optional<ANSWER> searchForAnswer(int index, boolean mayReadOverEagerly) {
        return searchSourceForAnswer();
    }

    protected Optional<ANSWER> searchSourceForAnswer() {
        if (answerSource == null) answerSource = answerSourceSupplier.get();
        while (answerSource.hasNext()) {
            ANSWER answer = answerSource.next();
            if (addIfAbsent(answer)) {
                if (reiterateOnAnswerAdded) requiresReiteration = true;
                return Optional.of(answer);
            }
        }
        if (!sourceCleared) setSourceExhausted();
        return Optional.empty();
    }

    protected boolean addIfAbsent(ANSWER answer) {
        if (answersSet.contains(answer)) return false;
        answers.add(answer);
        answersSet.add(answer);
        return true;
    }

    protected abstract List<SUBSUMES> answers();

    public class Reader extends AbstractPoller<ANSWER> {

        private final boolean mayReadOverEagerly;
        private int index;

        public Reader(boolean mayReadOverEagerly) {
            this.mayReadOverEagerly = mayReadOverEagerly;
            index = 0;
        }

        @Override
        public Optional<ANSWER> poll() {
            Optional<ANSWER> nextAnswer = get(index, mayReadOverEagerly);
            if (nextAnswer.isPresent()) index++;
            return nextAnswer;
        }

        @Override
        public void recycle() {}

    }

    public static class ConcludableAnswerCache extends AnswerCache<Concludable<?>, ConceptMap> {

        public ConcludableAnswerCache(Map<ConceptMap, AnswerCache<?, ConceptMap>> cacheRegister, ConceptMap state) {
            super(cacheRegister, state, Iterators::empty);
        }

        @Override
        public boolean isConcludableAnswerCache() {
            return true;
        }

        @Override
        protected List<ConceptMap> answers() {
            return iterate(answers).map(AnswerState::conceptMap).distinct().toList();
        }

    }

    public static abstract class SubsumptionAnswerCache<ANSWER, SUBSUMES> extends AnswerCache<ANSWER, SUBSUMES> {

        protected final Set<ConceptMap> subsumingConceptMaps;

        protected SubsumptionAnswerCache(Map<SUBSUMES, ? extends AnswerCache<?, SUBSUMES>> cacheRegister,
                                         ConceptMap state, Supplier<FunctionalIterator<ANSWER>> answerSource) {
            super(cacheRegister, state, answerSource);
            this.subsumingConceptMaps = subsumingConceptMaps(state);
        }

        private static Set<ConceptMap> subsumingConceptMaps(ConceptMap fromUpstream) {
            Set<ConceptMap> subsumingCacheKeys = new HashSet<>();
            Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>(fromUpstream.concepts());
            powerSet(concepts.entrySet()).forEach(powerSet -> subsumingCacheKeys.add(toConceptMap(powerSet)));
            subsumingCacheKeys.remove(fromUpstream);
            return subsumingCacheKeys;
        }

        private static <T> Set<Set<T>> powerSet(Set<T> set) {
            Set<Set<T>> powerSet = new HashSet<>();
            powerSet.add(set);
            set.forEach(el -> {
                Set<T> s = new HashSet<>(set);
                s.remove(el);
                powerSet.addAll(powerSet(s));
            });
            return powerSet;
        }

        private static ConceptMap toConceptMap(Set<Map.Entry<Identifier.Variable.Retrievable, Concept>> conceptsEntrySet) {
            HashMap<Identifier.Variable.Retrievable, Concept> map = new HashMap<>();
            conceptsEntrySet.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
            return new ConceptMap(map);
        }

        @Override
        protected Optional<ANSWER> searchForAnswer(int index, boolean mayReadOverEagerly) {
            if (completeIfSubsumerComplete()) {
                return get(index, mayReadOverEagerly);
            } else {
                return searchSourceForAnswer();
            }
        }

        public boolean completeIfSubsumerComplete() {
            Optional<AnswerCache<?, ANSWER>> completeSubsumer = findCompleteSubsumingCache();
            if (completeSubsumer.isPresent()) {
                completeFromSubsumer(completeSubsumer.get());
                return true;
            } else return false;
        }

        protected abstract Optional<AnswerCache<?, ANSWER>> findCompleteSubsumingCache();

        private void completeFromSubsumer(AnswerCache<?, ANSWER> subsumingCache) {
            setComplete();
            setCompletedAnswers(subsumingCache.answers());
            if (subsumingCache.requiresReiteration()) this.requiresReiteration = true;
        }

        private void setCompletedAnswers(List<ANSWER> completeAnswers) {
            completeAnswers.forEach(answer -> {
                if (subsumes(answer, state) && addIfAbsent(answer) && this.reiterateOnAnswerAdded) {
                    requiresReiteration = true;
                }
            });
        }

        protected abstract boolean subsumes(ANSWER answer, ConceptMap contained);

        public static class ConceptMapCache extends SubsumptionAnswerCache<ConceptMap, ConceptMap> {

            public ConceptMapCache(Map<ConceptMap, ? extends AnswerCache<?, ConceptMap>> cacheRegister,
                                   ConceptMap state, Supplier<FunctionalIterator<ConceptMap>> answerSource) {
                super(cacheRegister, state, answerSource);
            }

            @Override
            protected Optional<AnswerCache<?, ConceptMap>> findCompleteSubsumingCache() {
                for (ConceptMap subsumingCacheKey : subsumingConceptMaps) {
                    if (cacheRegister.containsKey(subsumingCacheKey)) {
                        AnswerCache<?, ConceptMap> subsumingCache;
                        if ((subsumingCache = cacheRegister.get(subsumingCacheKey)).isComplete()) {
                            // Gets the first complete cache we find. Getting the smallest could be more efficient.
                            return Optional.of(subsumingCache);
                        }
                    }
                }
                return Optional.empty();
            }

            @Override
            public boolean isConceptMapCache() {
                return true;
            }

            @Override
            public ConceptMapCache asConceptMapCache() {
                return this;
            }

            @Override
            protected List<ConceptMap> answers() {
                return answers;
            }

            @Override
            protected boolean subsumes(ConceptMap conceptMap, ConceptMap contained) {
                return conceptMap.concepts().entrySet().containsAll(contained.concepts().entrySet());
            }
        }
    }

}
