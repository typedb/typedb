/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.kb.log;

import grakn.core.Keyspace;
import grakn.core.concept.ConceptId;
import com.google.auto.value.AutoValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     Stores the commit log of a {@link grakn.core.GraknTx}.
 * </p>
 *
 * <p>
 *     Stores the commit log of a {@link grakn.core.GraknTx} which is uploaded to the jserver when the {@link grakn.core.GraknSession} is closed.
 *     The commit log is also uploaded periodically to make sure that if a failure occurs the counts are still roughly maintained.
 * </p>
 *
 */
@AutoValue
public abstract class CommitLog {

    public abstract Keyspace keyspace();

    public abstract Map<ConceptId, Long> instanceCount();

    public abstract Map<String, Set<ConceptId>> attributes();

    public static CommitLog create(Keyspace keyspace, Map<ConceptId, Long> instanceCount, Map<String, Set<ConceptId>> newAttributes){
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
