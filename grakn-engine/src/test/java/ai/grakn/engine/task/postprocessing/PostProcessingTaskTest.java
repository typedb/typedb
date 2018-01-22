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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.SystemKeyspace;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostProcessingTaskTest {
    private SystemKeyspace systemKeyspace;
    private IndexPostProcessor indexPostProcessor;
    private InstanceCountPostProcessor countPostProcessor;
    private PostProcessor postProcessor;
    private PostProcessingTask postProcessingTask;

    @Before
    public void setupMocks(){
        systemKeyspace = mock(SystemKeyspace.class);
        indexPostProcessor = mock(IndexPostProcessor.class);
        countPostProcessor = mock(InstanceCountPostProcessor.class);
        postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);

        GraknConfig config = mock(GraknConfig.class);
        when(config.getProperty(GraknConfigKey.POST_PROCESSOR_POOL_SIZE)).thenReturn(5);

        postProcessingTask = new PostProcessingTask(systemKeyspace, postProcessor, config);
    }

    @Test
    public void whenThereIsSomethingInTheIndexCache_PPStarts(){

    }

    @Test
    public void whenTheIndexCacheIsEmpty_NothingStarts(){

    }
}
