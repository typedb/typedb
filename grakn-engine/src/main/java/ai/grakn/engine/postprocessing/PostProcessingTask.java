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
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.util.Schema;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static ai.grakn.engine.GraknEngineConfig.POST_PROCESSING_DELAY;
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
public class PostProcessingTask implements BackgroundTask {

    public static final String POST_PROCESSING_LOCK = "post-processing-lock";

    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.LOG_NAME_POSTPROCESSING_DEFAULT);
    private static final GraknEngineConfig properties = GraknEngineConfig.getInstance();

    private PostProcessing postProcessing = PostProcessing.getInstance();
    private long maxTimeLapse = properties.getPropertyAsLong(POST_PROCESSING_DELAY);

    //TODO MAJOR Make this distributed in distributed environment
    private static final AtomicLong lastPPTaskCreated = new AtomicLong(System.currentTimeMillis());

    public PostProcessingTask(){
        lastPPTaskCreated.set(System.currentTimeMillis());
    }

    public PostProcessingTask(PostProcessing postProcessing, long maxTimeLapse){
        this.postProcessing = postProcessing;
        this.maxTimeLapse = maxTimeLapse;
    }

    /**
     * Run postprocessing only if enough time has passed since the last job was added
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        Instant lastJobAdded = Instant.ofEpochMilli(lastPPTaskCreated.get());
        long timeElapsed = Duration.between(lastJobAdded, now()).toMillis();

        LOG.info("Checking post processing should run: " + (timeElapsed >= maxTimeLapse));

        // Only try to run if enough time has passed
        if(timeElapsed > maxTimeLapse){

            Lock engineLock = LockProvider.getLock(POST_PROCESSING_LOCK);

            try {

                // Try to get lock for one second. If task cannot acquire lock, it should return successfully.
                boolean hasLock = engineLock.tryLock(1, TimeUnit.SECONDS);

                // If you have the lock, run (& return) PP and then release the lock
                if (hasLock) {
                    try {
                        String keyspace = configuration.at(KEYSPACE).asString();

                        runPostProcessing(keyspace, configuration.at(Schema.BaseType.CASTING.name()).asJsonMap(), postProcessing::performCastingFix);
                        runPostProcessing(keyspace, configuration.at(Schema.BaseType.RESOURCE.name()).asJsonMap(), postProcessing::performResourceFix);

                    } finally {
                        engineLock.unlock();
                    }
                }
            } catch (InterruptedException e){
                throw new RuntimeException(e);
            }

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

    private void runPostProcessing(String keyspace, Map<String, Json> conceptsByIndex,
                                   PostProcessing.Consumer<String, String, Set<ConceptId>> postProcessingMethod){

        for(String castingIndex:conceptsByIndex.keySet()){
            // Turn json
            Set<ConceptId> conceptIds = conceptsByIndex.get(castingIndex)
                    .asList().stream().map(ConceptId::of).collect(toSet());

            postProcessingMethod.apply(keyspace, castingIndex, conceptIds);
        }
    }
}
