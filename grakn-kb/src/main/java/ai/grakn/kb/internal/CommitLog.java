/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.kb.internal.concept.ThingImpl;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import mjson.Json;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Stores the commit log of a {@link ai.grakn.GraknTx}.
 * </p>
 *
 * <p>
 *     Stores the commit log of a {@link ai.grakn.GraknTx} which is uploaded to the server when the {@link ai.grakn.GraknSession} is closed.
 *     The commit log is also uploaded periodically to make sure that if a failure occurs the counts are still roughly maintained.
 * </p>
 */
public class CommitLog {
    private final Map<ConceptId, Long> newInstanceCount = new HashMap<>();
    private final Set<Attribute> newAttributes = new HashSet<>();


    public void addNewAttributes(Set<Attribute> attributes){
        newAttributes.addAll(attributes);
    }

    public void addNewInstances(Map<ConceptId, Long> instances){
        instances.forEach((key, value) -> newInstanceCount.merge(key, value, (v1, v2) -> v1 + v2));
    }

    private void clear(){
        newInstanceCount.clear();
        newAttributes.clear();
    }

    public Json getFormattedLog(){
        return formatLog(newInstanceCount, newAttributes);
    }

    /**
     * Returns the Formatted Log which is uploaded to the server.
     * @return a formatted Json log
     */
    public static Json formatLog(Map<ConceptId, Long> instances, Set<Attribute> attributes){
        //Concepts In Need of Inspection
        Json conceptsForInspection = Json.object();
        conceptsForInspection.set(Schema.BaseType.ATTRIBUTE.name(), loadConceptsForFixing(attributes));

        //Types with instance changes
        Json typesWithInstanceChanges = Json.array();

        instances.forEach((key, value) -> {
            Json jsonObject = Json.object();
            jsonObject.set(REST.Request.COMMIT_LOG_CONCEPT_ID, key.getValue());
            jsonObject.set(REST.Request.COMMIT_LOG_SHARDING_COUNT, value);
            typesWithInstanceChanges.add(jsonObject);
        });

        //Final Commit Log
        Json formattedLog = Json.object();
        formattedLog.set(REST.Request.COMMIT_LOG_FIXING, conceptsForInspection);
        formattedLog.set(REST.Request.COMMIT_LOG_COUNTING, typesWithInstanceChanges);

        return formattedLog;
    }
    private static <X extends Thing> Json loadConceptsForFixing(Set<X> instances){
        Map<String, Set<String>> conceptByIndex = new HashMap<>();
        instances.forEach(thing ->
                conceptByIndex.computeIfAbsent(((ThingImpl) thing).getIndex(), (e) -> new HashSet<>()).add(thing.getId().getValue()));
        return Json.make(conceptByIndex);
    }


}
