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
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class SubsumptionTracker {

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

    public Optional<ConceptMap> getSubsumer(ConceptMap unfinished) {
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
