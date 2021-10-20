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
import com.vaticle.typedb.core.reasoner.resolution.answer.Mapping;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class SubsumptiveCoordinator<RESOLVER extends SubsumptiveCoordinator<RESOLVER>> extends Resolver<RESOLVER> {

    private static final Logger LOG = LoggerFactory.getLogger(SubsumptiveCoordinator.class);
    private final Map<AnswerState.Partial<?>, Request.Factory> requestFactories;
    protected final Set<Mapping> equivalentMappings;
    protected final Map<ConceptMap, Mapping> reflexiveMappings;
    protected boolean isInitialised;

    protected SubsumptiveCoordinator(Driver<RESOLVER> driver, String name, Set<Mapping> equivalentMappings,
                                     ResolverRegistry registry) {
        super(driver, name, registry);
        this.isInitialised = false;
        this.requestFactories = new HashMap<>();
        this.equivalentMappings = equivalentMappings;
        this.reflexiveMappings = new HashMap<>();
    }

    @Override
    public void receiveVisit(Request.Visit fromUpstream) {
        LOG.trace("{}: received Visit: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;
        ConceptMap conceptMap = fromUpstream.partialAnswer().conceptMap();
        computeReflexiveMappings(conceptMap);
        Mapping mapping = reflexiveMappings.get(conceptMap);
        Driver<? extends Resolver<?>> worker = getOrCreateBoundResolver(fromUpstream.partialAnswer(),
                                                                        mapping.transform(conceptMap));
        Request.Factory requestFactory = getOrCreateRequestFactory(fromUpstream.partialAnswer(), worker, mapping);
        Request.Visit visit = requestFactory.createVisit(fromUpstream.trace());
        visitDownstream(visit, fromUpstream);
    }

    @Override
    public void receiveRevisit(Request.Revisit fromUpstream) {
        LOG.trace("{}: received Revisit: {}", name(), fromUpstream);
        assert isInitialised;
        if (isTerminated()) return;
        ConceptMap conceptMap = fromUpstream.visit().partialAnswer().conceptMap();
        computeReflexiveMappings(conceptMap);
        Mapping mapping = reflexiveMappings.get(conceptMap);
        Driver<? extends Resolver<?>> worker = getOrCreateBoundResolver(fromUpstream.visit().partialAnswer(),
                                                                        mapping.transform(conceptMap));
        Request.Factory requestFactory = getOrCreateRequestFactory(fromUpstream.visit().partialAnswer(), worker, mapping);
        Request.Revisit revisit = requestFactory.createRevisit(fromUpstream.trace(), fromUpstream.cycles());
        revisitDownstream(revisit, fromUpstream);
    }

    protected void computeReflexiveMappings(ConceptMap conceptMap) {
        if (reflexiveMappings.get(conceptMap) == null) {
            equivalentMappings.forEach(m -> reflexiveMappings.put(m.unTransform(conceptMap), m));
        }
    }

    private Request.Factory getOrCreateRequestFactory(AnswerState.Partial<?> partial, Driver<? extends Resolver<?>> receiver, Mapping mapping) {
        return requestFactories.computeIfAbsent(partial, p -> Request.Factory.create(driver(), receiver, applyRemapping(p, mapping)));
    }

    protected AnswerState.Partial<?> applyRemapping(AnswerState.Partial<?> partial, Mapping mapping) {
        // TODO: This is a no-op for Retrievable and Conclusion until they can be reflexively re-mapped.
        return partial;
    }

    @Override
    protected void receiveBlocked(Response.Blocked fromDownstream) {
        LOG.trace("{}: received Blocked: {}", name(), fromDownstream);
        if (isTerminated()) return;
        blockToUpstream(fromUpstream(fromDownstream.sourceRequest().visit()), fromDownstream.cycles());
    }

    protected abstract Driver<? extends Resolver<?>> getOrCreateBoundResolver(AnswerState.Partial<?> partial, ConceptMap mapped);  // TODO: partial answer only required for cycle detection

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
            Map<Retrievable, Concept> concepts = new HashMap<>(fromUpstream.concepts());
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

        private ConceptMap toConceptMap(Set<Map.Entry<Retrievable, Concept>> conceptsEntrySet) {
            HashMap<Retrievable, Concept> map = new HashMap<>();
            conceptsEntrySet.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
            return new ConceptMap(map);
        }
    }
}
