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

import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.AnswerCache;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class SubsumptiveCoordinator<
        RESOLVER extends SubsumptiveCoordinator<RESOLVER, WORKER>,
        WORKER extends Resolver<WORKER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(SubsumptiveCoordinator.class);
    protected final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, Driver<WORKER>>> workersByRoot; // TODO: We would like these not to be by root. They need to be, for now, for reiteration purposes.
    protected final Map<Driver<? extends Resolver<?>>, Map<ConceptMap, AnswerCache<?>>> cacheRegistersByRoot;
    protected boolean isInitialised;

    public SubsumptiveCoordinator(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name, registry);
        this.isInitialised = false;
        this.workersByRoot = new HashMap<>();
        this.cacheRegistersByRoot = new HashMap<>();
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;
        Driver<? extends Resolver<?>> root = fromUpstream.partialAnswer().root();
        Driver<WORKER> worker = getOrCreateWorker(root, fromUpstream.partialAnswer());
        Request.Template requestFactory = Request.Template.create(driver(), worker, fromUpstream.partialAnswer());
        Request.Visit visit = requestFactory.createVisit(fromUpstream.trace());
        visitDownstream(visit, fromUpstream);
    }

    @Override
    public void receiveRevisit(Request.Revisit fromUpstream) {
        LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
        assert isInitialised;
        if (isTerminated()) return;
        Driver<? extends Resolver<?>> root = fromUpstream.visit().partialAnswer().root();
        Driver<WORKER> worker = getOrCreateWorker(root, fromUpstream.visit().partialAnswer());
        Request.Template requestFactory = Request.Template.create(driver(), worker, fromUpstream.visit().partialAnswer());
        Request.Revisit revisit = requestFactory.createRevisit(fromUpstream.trace(), fromUpstream.cycles());
        revisitDownstream(revisit, fromUpstream);
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        blockToUpstream(fromUpstream(fromDownstream.sourceRequest().createVisit(fromDownstream.trace())),
                        fromDownstream.cycles());
    }

    abstract Driver<WORKER> getOrCreateWorker(Driver<? extends Resolver<?>> root, AnswerState.Partial<?> partial);

    @Override
    protected void receiveAnswer(Answer answer) {
        answerToUpstream(answer.answer(), upstreamRequest(answer));
    }

    @Override
    protected void receiveFail(Response.Fail fail) {
        failToUpstream(upstreamRequest(fail));
    }

    static class SubsumptionTracker {

        private final Map<ConceptMap, Set<ConceptMap>> subsumersMap;
        private final Set<ConceptMap> finishedStates;
        private final Map<ConceptMap, ConceptMap> finishedMapping;

        public SubsumptionTracker() {
            this.finishedStates = new HashSet<>();
            this.subsumersMap = new HashMap<>();
            this.finishedMapping = new HashMap<>();
        }

        public void addFinished(ConceptMap conceptMap) {
            this.finishedStates.add(conceptMap);
        }

        public Optional<ConceptMap> getFinishedSubsumer(ConceptMap unfinished) {
            if (finishedMapping.containsKey(unfinished)) return Optional.of(finishedMapping.get(unfinished));
            else {
                Optional<ConceptMap> finishedSubsumer = findFinishedSubsumer(
                        subsumersMap.computeIfAbsent(unfinished, this::subsumingConceptMaps));
                finishedSubsumer.ifPresent(finished -> finishedMapping.put(unfinished, finished));
                return finishedSubsumer;
            }
        }

        protected Optional<ConceptMap> findFinishedSubsumer(Set<ConceptMap> subsumers) {
            for (ConceptMap subsumer : subsumers) {
                // Gets the first complete cache we find. Getting the smallest could be more efficient.
                if (finishedStates.contains(subsumer)) return Optional.of(subsumer);
            }
            return Optional.empty();
        }

        private Set<ConceptMap> subsumingConceptMaps(ConceptMap fromUpstream) {
            Set<ConceptMap> subsumers = new HashSet<>();
            Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>(fromUpstream.concepts());
            powerSet(concepts.entrySet()).forEach(c -> subsumers.add(toConceptMap(c)));
            subsumers.remove(fromUpstream);
            return subsumers;
        }

        private <T> Set<Set<T>> powerSet(Set<T> set) {
            Set<Set<T>> powerSet = new HashSet<>();
            powerSet.add(set);
            set.forEach(el -> {
                Set<T> s = new HashSet<>(set);
                s.remove(el);
                powerSet.addAll(powerSet(s));
            });
            return powerSet;
        }

        private ConceptMap toConceptMap(Set<Map.Entry<Identifier.Variable.Retrievable, Concept>> conceptsEntrySet) {
            HashMap<Identifier.Variable.Retrievable, Concept> map = new HashMap<>();
            conceptsEntrySet.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
            return new ConceptMap(map);
        }
    }
}
