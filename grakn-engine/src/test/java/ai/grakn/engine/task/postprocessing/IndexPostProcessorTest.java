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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.log.CommitLog;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexPostProcessorTest {
    private static RedisIndexStorage indexStorage = mock(RedisIndexStorage.class);
    private static IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(mock(LockProvider.class), indexStorage);

    @BeforeClass
    public static void setupMocks(){
        indexStorage = mock(RedisIndexStorage.class);
        LockProvider mock = mock(LockProvider.class);
        when(mock.getLock(any())).thenReturn(new ReentrantLock());
        indexPostProcessor = IndexPostProcessor.create(mock, indexStorage);
    }

    @Test
    public void whenAddingCommitLogToPostProcessor_EnsureIndexStorageIsUpdated(){
        //Create Sample Data For CommitLog
        String index1 = "index1";
        String index2 = "index2";
        Set<ConceptId> ids1 = Arrays.asList("a", "b", "c").stream().map(ConceptId::of).collect(Collectors.toSet());
        Set<ConceptId> ids2 = Arrays. asList("1", "2", "3").stream().map(ConceptId::of).collect(Collectors.toSet());

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

    @Test
    public void whenPostProcessingIndices_EnsureFixingMethodIsCalled(){
        //Setup mocks
        Keyspace keyspace = Keyspace.of("whatakeyspace");

        EmbeddedGraknTx<?> tx = mock(EmbeddedGraknTx.class);
        when(tx.duplicateResourcesExist(any(), any())).thenReturn(true);

        when(tx.keyspace()).thenReturn(keyspace);

        String index = "index1";
        Set<ConceptId> ids = Stream.of("a", "b", "c").map(ConceptId::of).collect(Collectors.toSet());

        //Call post processor
        indexPostProcessor.mergeDuplicateConcepts(tx, index, ids);

        //Check method calls
        verify(tx, Mockito.times(1)).fixDuplicateResources(index, ids);
    }
}
