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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.kb.log.CommitLog;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IndexPostProcessorTest {
    private final RedisIndexStorage indexStorage = mock(RedisIndexStorage.class);
    private final IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(mock(LockProvider.class), indexStorage);


    @Test
    public void whenAddingCommitLogToPostProcessor_EnsureIndexStorageIsUpdated(){
        //Create Sample Data For CommitLog
        String index1 = "index1";
        String index2 = "index2";
        Set<ConceptId> ids1 = Arrays.asList("a", "b", "c").stream().map(ConceptId::of).collect(Collectors.toSet());
        Set<ConceptId> ids2 = Arrays.asList("1", "2", "3").stream().map(ConceptId::of).collect(Collectors.toSet());

        HashMap<String, Set<ConceptId>> attributes = new HashMap<>();
        attributes.put(index1, ids1);
        attributes.put(index2, ids2);

        //Create Commit Log
        Keyspace keyspace = Keyspace.of("whatakeyspace");
        CommitLog commitLog = CommitLog.create(keyspace, Collections.emptyMap(), attributes);

        //Call the post processor
        indexPostProcessor.updateIndices(commitLog);

        //Check index storage is updated
        verify(indexStorage, Mockito.times(1)).addIndex(keyspace, index1, ids1);
        verify(indexStorage, Mockito.times(1)).addIndex(keyspace, index2, ids2);
    }
}
