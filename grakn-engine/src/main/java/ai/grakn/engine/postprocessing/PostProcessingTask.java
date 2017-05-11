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
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.util.Schema;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static ai.grakn.util.REST.Request.COMMIT_LOG_FIXING;
import static java.util.stream.Collectors.toMap;
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
 * @author alexandraorth
 */
public class PostProcessingTask implements BackgroundTask {

    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);
    private static final String JOB_FINISHED = "Post processing Job [{}] completed for indeces and ids: [{}]";
    public static final String LOCK_KEY = "/post-processing-lock";

    /**
     * Apply CASTING and RESOURCE post processing jobs the concept ids in the provided configuration
     *
     * @param saveCheckpoint Checkpointing is not implemented in this task, so this parameter is not used.
     * @param configuration Configuration containing the IDs to be post processed.
     * @return True if successful.
     */
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        runPostProcessingMethod(configuration, Schema.BaseType.CASTING, this::runCastingFix);
        runPostProcessingMethod(configuration, Schema.BaseType.RESOURCE, this::runResourceFix);

        return true;
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Delayed task cannot be stopped while in progress");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Delayed task cannot be paused");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Delayed task cannot be resumed");
    }

    /**
     * Extract a map of concept indices to concept ids from the provided configuration
     *
     * @param type Type of concept to extract. This correlates to the key in the provided configuration.
     * @param configuration Configuration from which to extract the configuration.
     * @return Map of concept indices to ids that has been extracted from the provided configuration.
     */
    private Map<String,Set<ConceptId>> conceptFromConfig(Schema.BaseType type, TaskConfiguration configuration) {
        return configuration.json().at(COMMIT_LOG_FIXING).at(type.name()).asJsonMap().entrySet().stream().collect(toMap(
                Map.Entry::getKey,
                e -> e.getValue().asList().stream().map(ConceptId::of).collect(toSet())
        ));
    }

    /**
     * Main method which attempts to run all post processing jobs.
     *
     * @param postProcessingMethod The post processing job.
     *                      Either {@link ai.grakn.engine.postprocessing.PostProcessingTask#runResourceFix(GraknGraph, String, Set)} or
     *                      {@link ai.grakn.engine.postprocessing.PostProcessingTask#runCastingFix(GraknGraph, String, Set)}.
     *                      This then returns a function which will complete the job after going through validation

     */
    private void runPostProcessingMethod(TaskConfiguration configuration, Schema.BaseType baseType,
                                         TriFunction<GraknGraph, String, Set<ConceptId>, Boolean> postProcessingMethod){

        Map<String, Set<ConceptId>> allToPostProcess = conceptFromConfig(baseType, configuration);

        allToPostProcess.entrySet().forEach(e -> {
            String conceptIndex = e.getKey();
            Set<ConceptId> conceptIds = e.getValue();

            GraphMutators.runGraphMutationWithRetry(configuration,
                    (graph) -> runPostProcessingMethod(graph, conceptIndex, conceptIds, postProcessingMethod));
        });

        LOG.debug(JOB_FINISHED, baseType.name(), allToPostProcess);
    }

    /**
     * Apply the given post processing method to the provided concept index and set of ids.
     *
     * @param graph
     * @param conceptIndex
     * @param conceptIds
     * @param postProcessingMethod
     */
    public void runPostProcessingMethod(GraknGraph graph, String conceptIndex, Set<ConceptId> conceptIds,
                                         TriFunction<GraknGraph, String, Set<ConceptId>, Boolean> postProcessingMethod){

        if(postProcessingMethod.apply(graph, conceptIndex, conceptIds)) {
            validateMerged(graph, conceptIndex, conceptIds).
                    ifPresent(message -> {
                        throw new RuntimeException(message);
                    });

            graph.admin().commitNoLogs();
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
}