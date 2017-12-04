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

package ai.grakn.kb.internal.log;

import ai.grakn.concept.ConceptId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Stores the commit log of a {@link ai.grakn.GraknTx}.
 * </p>
 *
 * <p>
 *     Stores the commit log of a {@link ai.grakn.GraknTx} which is uploaded to the server when the {@link ai.grakn.GraknSession} is closed.
 *     The commit log is also uploaded periodically to make sure that if a failure occurs the counts are still roughly maintained.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class CommitLog {

    @JsonProperty("instance-count")
    public abstract Map<ConceptId, Long> instanceCount();

    @JsonProperty("new-attributes")
    public abstract Map<String, Set<ConceptId>> attributes();

    @JsonCreator
    public static CommitLog create(
            @JsonProperty("instance-count") Map<ConceptId, Long> instanceCount,
            @JsonProperty("new-attributes") Map<String, Set<ConceptId>> newAttributes
    ){
        return new AutoValue_CommitLog(instanceCount, newAttributes);
    }

    /**
     * Creates a {@link CommitLog} which is safe to be mutated by multiple transactions
     * @return a thread safe {@link CommitLog}
     */
    public static CommitLog createThreadSafe(){
        return create(new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    }

    public void clear(){
        instanceCount().clear();
        attributes().clear();
    }
}
