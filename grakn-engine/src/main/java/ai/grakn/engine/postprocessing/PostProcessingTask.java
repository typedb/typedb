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
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskSubmitter;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import mjson.Json;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <p>
 *     Task that control when postprocessing starts.
 * </p>
 *
 * <p>
 *     This task begins only if enough time has passed (configurable) since the last time a job was added.
 * </p>
 *
 * @author alexandraorth, fppt
 */
public class PostProcessingTask extends BackgroundTask {
    private static final GraknEngineConfig CONFIG = GraknEngineConfig.getInstance();
    private static final int PP_TASK_DELAY_MS = CONFIG.getPropertyAsInt(GraknEngineConfig.POST_PROCESSING_TASK_DELAY);
    private static final EngineGraknGraphFactory FACTORY = EngineGraknGraphFactory.create(CONFIG.getProperties());
    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);
    private static final String JOB_FINISHED = "Post processing Job [{}] completed for indeces and ids: [{}]";
    private static final String LOCK_KEY = "/post-processing-lock";

    /**
     * Apply CASTING and RESOURCE post processing jobs the concept ids in the provided configuration
     *
     * @param saveCheckpoint Checkpointing is not implemented in this task, so this parameter is not used.
     * @param configuration Configuration containing the IDs to be post processed.
     * @return True if successful.
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration, TaskSubmitter taskSubmitter) {
        runPostProcessingMethod(configuration, Schema.BaseType.CASTING, this::duplicateCastingsExist, this::runCastingFix);
        runPostProcessingMethod(configuration, Schema.BaseType.RESOURCE, this::duplicateResourcesExist, this::runResourceFix);

        return true;
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Delayed task cannot be resumed");
    }

    /**
     * Main method which attempts to run all post processing jobs.
     *
     * @param duplicatesExistMethod Method to determine if there are duplicates to be post processed.
     *                      Either {@link ai.grakn.engine.postprocessing.PostProcessingTask#duplicateCastingsExist(GraknGraph, String, Set)} or
     *                      {@link ai.grakn.engine.postprocessing.PostProcessingTask#duplicateResourcesExist(GraknGraph, String, Set)}
     * @param postProcessingMethod The post processing job to be executed if duplicates exist
     *                      Either {@link ai.grakn.engine.postprocessing.PostProcessingTask#runResourceFix(GraknGraph, String, Set)} or
     *                      {@link ai.grakn.engine.postprocessing.PostProcessingTask#runCastingFix(GraknGraph, String, Set)}.
     */
    private void runPostProcessingMethod(TaskConfiguration configuration, Schema.BaseType baseType,
                                         TriFunction<GraknGraph, String, Set<ConceptId>, Boolean> duplicatesExistMethod,
                                         TriFunction<GraknGraph, String, Set<ConceptId>, Boolean> postProcessingMethod){

        Map<String, Set<ConceptId>> allToPostProcess = getPostProcessingJobs(baseType, configuration);

        allToPostProcess.entrySet().forEach(e -> {
            String conceptIndex = e.getKey();
            Set<ConceptId> conceptIds = e.getValue();

            GraphMutators.runGraphMutationWithRetry(FACTORY, configuration.json().at(REST.Request.KEYSPACE).asString(),
                    (graph) -> runPostProcessingMethod(graph, conceptIndex, conceptIds, duplicatesExistMethod, postProcessingMethod));

        });

        LOG.debug(JOB_FINISHED, baseType.name(), allToPostProcess);
    }

    /**
     * Extract a map of concept indices to concept ids from the provided configuration
     *
     * @param type Type of concept to extract. This correlates to the key in the provided configuration.
     * @param configuration Configuration from which to extract the configuration.
     * @return Map of concept indices to ids that has been extracted from the provided configuration.
     */
    private static Map<String,Set<ConceptId>> getPostProcessingJobs(Schema.BaseType type, TaskConfiguration configuration) {
        return configuration.json().at(REST.Request.COMMIT_LOG_FIXING).at(type.name()).asJsonMap().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().asList().stream().map(ConceptId::of).collect(Collectors.toSet())
        ));
    }

    /**
     * Apply the given post processing method to the provided concept index and set of ids.
     *
     * @param graph
     * @param conceptIndex
     * @param conceptIds
     * @param duplicatesExistMethod
     * @param postProcessingMethod
     */
    private void runPostProcessingMethod(GraknGraph graph, String conceptIndex, Set<ConceptId> conceptIds,
                                         TriFunction<GraknGraph, String, Set<ConceptId>, Boolean> duplicatesExistMethod,
                                         TriFunction<GraknGraph, String, Set<ConceptId>, Boolean> postProcessingMethod){

        if(duplicatesExistMethod.apply(graph, conceptIndex, conceptIds)){

            // Acquire a lock when you post process on an index to prevent race conditions
            // Lock is acquired after checking for duplicates to reduce runtime
            Lock indexLock = LockProvider.getLock(PostProcessingTask.LOCK_KEY + "/" + conceptIndex);
            indexLock.lock();

            try {
                // execute the provided post processing method
                postProcessingMethod.apply(graph, conceptIndex, conceptIds);

                // ensure post processing was correctly executed
                validateMerged(graph, conceptIndex, conceptIds).
                        ifPresent(message -> {
                            throw new RuntimeException(message);
                        });

                // persist merged concepts
                graph.admin().commitNoLogs();
            } finally {
                indexLock.unlock();
            }
        }
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

    /**
     * Run a a resource duplication merge on the provided concepts
     * @param graph Graph on which to apply the fixes
     * @param index The unique index of the concept which must exist at the end
     * @param conceptIds The conceptIds which effectively need to be merged.
     */
    private boolean runResourceFix(GraknGraph graph, String index, Set<ConceptId> conceptIds) {
        return graph.admin().fixDuplicateResources(index, conceptIds);
    }


    /**
     * Run a casting duplication merge job on the provided concepts
     * @param graph Graph on which to apply the fixes
     * @param index The unique index of the concept which must exist at the end
     * @param conceptIds The conceptIds which effectively need to be merged.
     */
    private boolean runCastingFix(GraknGraph graph, String index, Set<ConceptId> conceptIds) {
        return graph.admin().fixDuplicateCastings(index, conceptIds);
    }

    /**
     * Check if there are duplicate castings for the given index
     * @param graph Graph on which to apply the fixes
     * @param index The unique index of the concept that may have duplicates
     * @param conceptIds The conceptIds which may be the duplicates
     */
    private boolean duplicateCastingsExist(GraknGraph graph, String index, Set<ConceptId> conceptIds) {
        return graph.admin().duplicateCastingsExist(index, conceptIds);
    }


    /**
     * Check if there are duplicate resources for the given index

     * @param graph Graph on which to apply the fixes
     * @param index The unique index of the concept that may have duplicates
     * @param conceptIds The conceptIds which may be the duplicates
     */
    private boolean duplicateResourcesExist(GraknGraph graph, String index, Set<ConceptId> conceptIds) {
        return graph.admin().duplicateResourcesExist(index, conceptIds);
    }

    /**
     * Helper method which creates PP Task States.
     *
     * @param creator The class which is creating the task
     * @return The executable postprocessing task state
     */
    public static TaskState createTask(Class creator){
        return TaskState.of(PostProcessingTask.class,
                creator.getName(),
                TaskSchedule.at(Instant.now().plusMillis(PP_TASK_DELAY_MS)),
                TaskState.Priority.LOW);
    }

    /**
     * Helper method which creates the task config needed in order to execute a PP task
     *
     * @param keyspace The keyspace of the graph to execute this on.
     * @param config The config which contains the concepts to post process
     * @return The task configuration encapsulating the above details in a manner executable by the task runner
     */
    public static TaskConfiguration createConfig(String keyspace, String config){
        Json postProcessingConfiguration = Json.object();
        postProcessingConfiguration.set(REST.Request.KEYSPACE, keyspace);
        postProcessingConfiguration.set(REST.Request.COMMIT_LOG_FIXING, Json.read(config).at(REST.Request.COMMIT_LOG_FIXING));
        return TaskConfiguration.of(postProcessingConfiguration);
    }
}