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

package ai.grakn.engine.postprocessing;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.storage.LockingBackgroundTask;
import ai.grakn.graph.admin.ConceptCache;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

import static ai.grakn.engine.GraknEngineConfig.POST_PROCESSING_DELAY;
import static java.time.Instant.now;

/**
 * <p>
 *     Task that control when postprocessing starts.
 * </p>
 *
 * <p>
 *     This task begins only if enough time has passed (configurable) since the last time a job was added.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public class PostProcessingTask extends LockingBackgroundTask {
    public static final String LOCK_KEY = "post-processing-lock";
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.LOG_NAME_POSTPROCESSING_DEFAULT);
    private static final GraknEngineConfig properties = GraknEngineConfig.getInstance();
    private static final ConceptCache cache = EngineCacheProvider.getCache();

    private PostProcessing postProcessing = PostProcessing.getInstance();
    private long maxTimeLapse = properties.getPropertyAsLong(POST_PROCESSING_DELAY);

    //TODO: Get rid of these constructors. They only used in tests
    public PostProcessingTask(){};

    public PostProcessingTask(PostProcessing postProcessing, long maxTimeLapse){
        this.postProcessing = postProcessing;
        this.maxTimeLapse = maxTimeLapse;
    }

    /**
     * Run postprocessing only if enough time has passed since the last job was added
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        Instant lastJobAdded = Instant.ofEpochMilli(cache.getLastTimeJobAdded());
        long timeElapsed = Duration.between(lastJobAdded, now()).toMillis();

        LOG.trace("Checking post processing should run: " + (timeElapsed >= maxTimeLapse));

        // Only try to run if enough time has passed
        if(timeElapsed > maxTimeLapse){
            super.start(saveCheckpoint, configuration);
        }

        return true;
    }

    @Override
    public boolean stop() {
        return postProcessing.stop();
    }

    @Override
    public void pause() {}

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        return false;
    }

    @Override
    protected String getLockingKey(){
        return LOCK_KEY;
    }

    @Override
    protected boolean runLockingBackgroundTask(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration){
        return postProcessing.run();
    }

}
