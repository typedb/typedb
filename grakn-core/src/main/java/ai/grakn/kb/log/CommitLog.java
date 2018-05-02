/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.log;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import java.util.HashMap;
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

    @JsonProperty
    public abstract Keyspace keyspace();

    @JsonProperty("instance-setNumber")
    public abstract Map<ConceptId, Long> instanceCount();

    @JsonProperty("new-attributes")
    public abstract Map<String, Set<ConceptId>> attributes();

    @JsonCreator
    public static CommitLog create(
            @JsonProperty("keyspace") Keyspace keyspace,
            @JsonProperty("instance-setNumber") Map<ConceptId, Long> instanceCount,
            @JsonProperty("new-attributes") Map<String, Set<ConceptId>> newAttributes
    ){
        return new AutoValue_CommitLog(keyspace, instanceCount, newAttributes);
    }

    /**
     * Creates a {@link CommitLog} which is safe to be mutated by multiple transactions
     * @return a thread safe {@link CommitLog}
     */
    public static CommitLog createThreadSafe(Keyspace keyspace){
        return create(keyspace, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    }

    /**
     * Creates a {@link CommitLog} which should only be used by a single transaction.
     * @return a simple {@link CommitLog}
     */
    public static CommitLog createDefault(Keyspace keyspace){
        return create(keyspace, new HashMap<>(), new HashMap<>());
    }

    public void clear(){
        instanceCount().clear();
        attributes().clear();
    }
}
