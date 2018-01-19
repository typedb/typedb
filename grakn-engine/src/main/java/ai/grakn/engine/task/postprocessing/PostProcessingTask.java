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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * <p>
 *     Class which facilitates running {@link PostProcessor} jobs.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class PostProcessingTask {
    private final PostProcessor postProcessor;
    private final ScheduledExecutorService threadPool;

    public PostProcessingTask(PostProcessor postProcessor, GraknConfig config){
        this.postProcessor = postProcessor;
        this.threadPool = Executors.newScheduledThreadPool(config.getProperty(GraknConfigKey.POST_PROCESSOR_POOL_SIZE));
    }

    public void close(){
        threadPool.shutdown();
    }

}
