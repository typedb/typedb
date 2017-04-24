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

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.storage.LockingBackgroundTask;
import ai.grakn.util.Schema;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static ai.grakn.engine.GraknEngineConfig.POST_PROCESSING_DELAY;
import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static java.time.Instant.now;
import static java.util.stream.Collectors.toSet;

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

    private PostProcessing postProcessing = PostProcessing.getInstance();
    private long maxTimeLapse = properties.getPropertyAsLong(POST_PROCESSING_DELAY);

    //TODO MAJOR Make this distributed in distributed environment
    public static final AtomicLong lastPPTaskCreated = new AtomicLong(System.currentTimeMillis());

    public PostProcessingTask(){

    }

    public PostProcessingTask(PostProcessing postProcessing, long maxTimeLapse){
        this.postProcessing = postProcessing;
        this.maxTimeLapse = maxTimeLapse;
    }

    /**
     * Run postprocessing only if enough time has passed since the last job was added
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        Instant lastJobAdded = Instant.ofEpochMilli(lastPPTaskCreated.get());
        long timeElapsed = Duration.between(lastJobAdded, now()).toMillis();

        LOG.trace("Checking post processing should run: " + (timeElapsed >= maxTimeLapse));

        // Only try to run if enough time has passed
        if(timeElapsed > maxTimeLapse){
            return super.start(saveCheckpoint, configuration);
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
    public boolean runLockingBackgroundTask(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration){
        String keyspace = configuration.json().at(KEYSPACE).asString();

        Json innerConfig = configuration.json().at(COMMIT_LOG_FIXING);
        runPostProcessing(keyspace, innerConfig.at(Schema.BaseType.CASTING.name()).asJsonMap(), postProcessing::performCastingFix);
        runPostProcessing(keyspace, innerConfig.at(Schema.BaseType.RESOURCE.name()).asJsonMap(), postProcessing::performResourceFix);

        return false;
    }


    private void runPostProcessing(String keyspace, Map<String, Json> conceptsByIndex,
                                   PostProcessing.Consumer<String, String, Set<ConceptId>> postProcessingMethod){

        for(Map.Entry<String, Json> castingIndex:conceptsByIndex.entrySet()){
            // Turn json
            Set<ConceptId> conceptIds = castingIndex.getValue().asList().stream().map(ConceptId::of).collect(toSet());
            postProcessingMethod.apply(keyspace, castingIndex.getKey(), conceptIds);
        }
    }
}
