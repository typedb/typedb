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

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class AnswerCache<ANSWER, SUBSUMES> {

    protected final List<ANSWER> answers;
    private final Set<ANSWER> answersSet;
    private boolean reexploreOnNewAnswers;
    private boolean requiresReexploration;
    protected FunctionalIterator<ANSWER> unexploredAnswers;
    protected boolean complete;
    protected final Map<SUBSUMES, ? extends AnswerCache<?, SUBSUMES>> cacheRegister;
    protected final ConceptMap state;

    protected AnswerCache(Map<SUBSUMES, ? extends AnswerCache<?, SUBSUMES>> cacheRegister, ConceptMap state) {
        this.cacheRegister = cacheRegister;
        this.state = state;
        this.unexploredAnswers = Iterators.empty();
        this.answers = new ArrayList<>(); // TODO: Replace answer list and deduplication set with a bloom filter
        this.answersSet = new HashSet<>();
        this.reexploreOnNewAnswers = false;
        this.requiresReexploration = false;
        this.complete = false;
    }

    public void add(ANSWER newAnswer) {
        addIfAbsent(newAnswer);
    }

    public void addSource(FunctionalIterator<ANSWER> newAnswers) {
        // assert !isComplete(); // Removed to allow additional answers to propagate upstream, which crucially may be
        // carrying requiresReiteration flags
        unexploredAnswers = unexploredAnswers.link(newAnswers);
    }

    public Poller<ANSWER> reader(boolean isSubscriber) {
        return new Reader(isSubscriber);
    }

    public void setComplete() {
        complete = true;
        unexploredAnswers.recycle();
    }

    public boolean isComplete() {
        return complete;
    }

    public void setRequiresReexploration() {
        this.requiresReexploration = true;
    }

    public boolean requiresReexploration() {
        return requiresReexploration;
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
            if (!nextAnswer.isPresent() && isSubscriber) reexploreOnNewAnswers = true;
            return nextAnswer;
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    protected Optional<ANSWER> searchForAnswer(int index, boolean mayReadOverEagerly) {
        return searchUnexploredForAnswer();
    }

    protected Optional<ANSWER> searchUnexploredForAnswer() {
        while (unexploredAnswers.hasNext()) {
            Optional<ANSWER> nextAnswer = addIfAbsent(unexploredAnswers.next());
            if (nextAnswer.isPresent()) return nextAnswer;
        }
        return Optional.empty();
    }

    protected Optional<ANSWER> addIfAbsent(ANSWER answer) {
        if (answersSet.contains(answer)) return Optional.empty();
        answers.add(answer);
        answersSet.add(answer);
        if (reexploreOnNewAnswers) this.requiresReexploration = true;
        return Optional.of(answer);
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

    }

    public static class ConcludableAnswerCache extends AnswerCache<Concludable<?>, ConceptMap> {

        public ConcludableAnswerCache(Map<ConceptMap, AnswerCache<?, ConceptMap>> cacheRegister, ConceptMap state) {
            super(cacheRegister, state);
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

        protected SubsumptionAnswerCache(Map<SUBSUMES, ? extends AnswerCache<?, SUBSUMES>> cacheRegister, ConceptMap state) {
            super(cacheRegister, state);
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
            Optional<AnswerCache<?, ANSWER>> subsumingCache;
            if ((subsumingCache = findCompleteSubsumingCache()).isPresent()) {
                completeFromSubsumer(subsumingCache.get());
                return get(index, mayReadOverEagerly);
            } else {
                return searchUnexploredForAnswer();
            }
        }

        protected abstract Optional<AnswerCache<?, ANSWER>> findCompleteSubsumingCache();

        private void completeFromSubsumer(AnswerCache<?, ANSWER> subsumingCache) {
            setCompletedAnswers(subsumingCache.answers());
            setComplete();
            if (subsumingCache.requiresReexploration()) setRequiresReexploration();
        }

        private void setCompletedAnswers(List<ANSWER> completeAnswers) {
            iterate(completeAnswers).filter(e -> subsumes(e, state)).toList().forEach(this::addIfAbsent);
        }

        protected abstract boolean subsumes(ANSWER answer, ConceptMap contained);

        public static class ConceptMapCache extends SubsumptionAnswerCache<ConceptMap, ConceptMap> {

            public ConceptMapCache(Map<ConceptMap, ? extends AnswerCache<?, ConceptMap>> cacheRegister,
                                   ConceptMap state) {
                super(cacheRegister, state);
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
