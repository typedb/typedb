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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.task.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.GraknKeyspaceStore;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostProcessingTaskTest {
    private Keyspace keyspaceA = Keyspace.of("a");
    private Set<Keyspace> keyspaces = new HashSet<>(Arrays.asList(keyspaceA, Keyspace.of("b"), Keyspace.of("c")));
    private EngineGraknTxFactory factory;
    private IndexPostProcessor indexPostProcessor;
    private GraknConfig config;
    private PostProcessor postProcessor;
    private PostProcessingTask postProcessingTask;

    @Before
    public void setupMocks(){
        GraknKeyspaceStore graknKeyspaceStore = mock(GraknKeyspaceStore.class);
        when(graknKeyspaceStore.keyspaces()).thenReturn(keyspaces);

        factory = mock(EngineGraknTxFactory.class);
        when(factory.keyspaceStore()).thenReturn(graknKeyspaceStore);

        indexPostProcessor = mock(IndexPostProcessor.class);
        postProcessor = PostProcessor.create(indexPostProcessor, mock(CountPostProcessor.class));

        config = mock(GraknConfig.class);
        when(config.getProperty(GraknConfigKey.POST_PROCESSOR_POOL_SIZE)).thenReturn(5);
        when(config.getProperty(GraknConfigKey.POST_PROCESSOR_DELAY)).thenReturn(1);

        postProcessingTask = new PostProcessingTask(factory, indexPostProcessor, config);
    }

    @Test
    public void whenThereIsSomethingInTheIndexCache_PPStarts() throws InterruptedException {
        //Configure Data For Mocks
        String index1 = "index1";
        when(indexPostProcessor.popIndex(keyspaceA)).thenReturn(index1).thenReturn(null);

        Set<ConceptId> ids = Stream.of("id1", "id2", "id3").map(ConceptId::of).collect(Collectors.toSet());
        when(indexPostProcessor.popIds(keyspaceA, index1)).thenReturn(ids);

        //Run the method
        postProcessingTask.run();

        //Give time for PP to run
        Thread.sleep(config.getProperty(GraknConfigKey.POST_PROCESSOR_DELAY) * 2000);

        //Check methods are called
        verify(indexPostProcessor, Mockito.times(1)).mergeDuplicateConcepts(any(), eq(index1), eq(ids));
    }

    @Test
    public void whenTheIndexCacheIsEmpty_NothingStarts(){
        //Run the method
        postProcessingTask.run();

        //Check no methods calls
        verify(indexPostProcessor, Mockito.times(0)).mergeDuplicateConcepts(any(), any(), any());
    }

    @Test
    public void whenThereAreMultipleIndicesToPostProcess_EnsurePPIsCalledMultipleTimes() throws InterruptedException {
        String index1 = "index1";
        String index2 = "index2";
        String index3 = "index3";
        String index4 = "index4";

        when(indexPostProcessor.popIndex(keyspaceA)).
                thenReturn(index1).
                thenReturn(index2).
                thenReturn(index3).
                thenReturn(index4).
                thenReturn(null);

        Set<ConceptId> ids = Stream.of("id1", "id2", "id3").map(ConceptId::of).collect(Collectors.toSet());
        when(indexPostProcessor.popIds(keyspaceA, index1)).thenReturn(ids);
        when(indexPostProcessor.popIds(keyspaceA, index2)).thenReturn(ids);
        when(indexPostProcessor.popIds(keyspaceA, index3)).thenReturn(ids);
        when(indexPostProcessor.popIds(keyspaceA, index4)).thenReturn(ids);

        //Run the method
        postProcessingTask.run();

        //Give time for PP to run
        Thread.sleep(config.getProperty(GraknConfigKey.POST_PROCESSOR_DELAY) * 2000);

        //Check methods are called
        verify(indexPostProcessor, Mockito.times(4)).mergeDuplicateConcepts(any(), any(), eq(ids));
    }
}
