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

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.storage.LockingBackgroundTask;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import java.util.Optional;
import mjson.Json;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;
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

    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);

    private static final long MAX_RETRY = 10;
    private long maxTimeLapse = GraknEngineConfig.getInstance().getPropertyAsLong(POST_PROCESSING_DELAY);

    //TODO MAJOR Make this distributed in distributed environment
    public static final AtomicLong lastPPTaskCreated = new AtomicLong(System.currentTimeMillis());

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
        throw new UnsupportedOperationException("Post processing cannot be stopped while in progress");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Post processing cannot be paused");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        return false;
    }

    @Override
    public String getLockingKey(){
        return LOCK_KEY;
    }

    @Override
    public boolean runLockingBackgroundTask(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        String keyspace = configuration.json().at(KEYSPACE).asString();

        applyPPToMapEntry(configuration, Schema.BaseType.CASTING.name(), keyspace, PostProcessingTask::runCastingFix);
        applyPPToMapEntry(configuration, Schema.BaseType.RESOURCE.name(), keyspace, PostProcessingTask::runResourceFix);

        return false;
    }

    public void applyPPToMapEntry(TaskConfiguration configuration, String type, String keyspace,
                TriConsumer<GraknGraph, String, Set<ConceptId>> postProcessingMethod){

        Json innerConfig = configuration.json().at(COMMIT_LOG_FIXING);
        Map<String, Json> conceptsByIndex = innerConfig.at(type).asJsonMap();

        for(Map.Entry<String, Json> castingIndex:conceptsByIndex.entrySet()){
            // Turn json
            Set<ConceptId> conceptIds = castingIndex.getValue().asList().stream().map(ConceptId::of).collect(toSet());

            runPostProcessingJob((graph) -> postProcessingMethod.accept(graph, castingIndex.getKey(), conceptIds),
                    keyspace, castingIndex.getKey(), conceptIds);
        }
    }

    /**
     * Main method which attempts to run all post processing jobs.
     *
     * @param postProcessor The post processing job.
     *                      Either {@link ai.grakn.engine.postprocessing.PostProcessingTask#runResourceFix(GraknGraph, String, Set)} or
     *                      {@link ai.grakn.engine.postprocessing.PostProcessingTask#runCastingFix(GraknGraph, String, Set)}.
     *                      This then returns a function which will complete the job after going through validation
     * @param keyspace The keyspace to post process against.
     * @param conceptIndex The unique index of the concept which must exist at the end
     * @param conceptIds The conceptIds which effectively need to be merged.
     */
    public void runPostProcessingJob(Consumer<GraknGraph> postProcessor,
                                             String keyspace, String conceptIndex, Set<ConceptId> conceptIds){
        boolean notDone = true;
        int retry = 0;

        while (notDone) {
            //Try to Fix the job
            try(GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE))  {

                //Perform the fix
                postProcessor.accept(graph);

                //Check if the fix worked
                validateMerged(graph, conceptIndex, conceptIds).
                        ifPresent(message -> {
                            throw new RuntimeException(message);
                        });

                //Commit the fix
                graph.admin().commitNoLogs();

                return; //If it can get here. All post processing has succeeded.

            } catch (Throwable t) { //These exceptions need to become more specialised
                LOG.error(ErrorMessage.POSTPROCESSING_ERROR.getMessage(t.getMessage()), t);
            }

            //Fixing the job has failed after several attempts
            if (retry > MAX_RETRY) {
                notDone = false;
            }

            retry = performRetry(retry);
        }

        LOG.error(ErrorMessage.UNABLE_TO_ANALYSE_CONCEPT.getMessage(conceptIds));
    }

    public void setTimeLapse(long time){
        this.maxTimeLapse = time;
    }

    /**
     * Checks that post processing was done successfully by doing two things:
     *  1. That there is only 1 valid conceptID left
     *  2. That the concept Index does not return null
     * @param graph A grakn graph to run the checks against.
     * @param conceptIndex The concept index which MUST return a valid concept
     * @param conceptIds The concpet ids which should only return 1 valid concept
     * @return An error if one of the above rules are not satisfied.
     */
    private Optional<String> validateMerged(GraknGraph graph, String conceptIndex, Set<ConceptId> conceptIds){
        //Check number of valid concept Ids
        int numConceptFound = 0;
        for (ConceptId conceptId : conceptIds) {
            if (graph.getConcept(conceptId) != null) {
                numConceptFound++;
                if (numConceptFound > 1) {
                    StringBuilder conceptIdValues = new StringBuilder();
                    for (ConceptId id : conceptIds) {
                        conceptIdValues.append(id.getValue()).append(",");
                    }
                    return Optional.of("Not all concept were merged. The set of concepts [" + conceptIds.size() + "] with IDs [" + conceptIdValues.toString() + "] matched more than one concept");
                }
            }
        }

        //Check index
        if(graph.admin().getConcept(Schema.ConceptProperty.INDEX, conceptIndex) == null){
            return Optional.of("The concept index [" + conceptIndex + "] did not return any concept");
        }

        return Optional.empty();
    }

    static void runResourceFix(GraknGraph graph, String index, Set<ConceptId> conceptIds) {
        graph.admin().fixDuplicateResources(index, conceptIds);
    }

    static void runCastingFix(GraknGraph graph, String index, Set<ConceptId> conceptIds) {
        graph.admin().fixDuplicateCastings(index, conceptIds);
    }

    private int performRetry(int retry){
        retry ++;
        double seed = 1.0 + (Math.random() * 5.0);
        double waitTime = (retry * 2.0)  + seed;
        LOG.debug(ErrorMessage.BACK_OFF_RETRY.getMessage(waitTime));

        try {
            Thread.sleep((long) Math.ceil(waitTime * 1000));
        } catch (InterruptedException e1) {
            LOG.error("Exception",e1);
        }

        return retry;
    }
}
