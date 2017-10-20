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

import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.GraknConfigKey;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.codahale.metrics.Timer.Context;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

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
    private static final Logger LOG = LoggerFactory.getLogger(PostProcessingTask.class);
    private static final String JOB_FINISHED = "Post processing Job [{}] completed for indeces and ids: [{}]";
    private static final String ATTRIBUTE_LOCK_KEY = "/post-processing-attribute-lock";
    private static final String RELATIONSHIP_LOCK_KEY = "/post-processing-relationship-lock";

    /**
     * Apply {@link ai.grakn.concept.Attribute} post processing jobs the concept ids in the provided configuration
     *
     * @return True if successful.
     */
    @Override
    public boolean start() {
        try (Context context = metricRegistry().timer(name(PostProcessingTask.class, "execution")).time()) {
            Json logs = configuration().json();
            if(logs.has(REST.Request.COMMIT_LOG_FIXING)){
                logs = logs.at(REST.Request.COMMIT_LOG_FIXING);
                Keyspace keyspace = Keyspace.of(configuration().json().at(REST.Request.KEYSPACE).asString());
                int fixingRetries = engineConfiguration().getProperty(GraknConfigKey.LOADER_REPEAT_COMMITS);

                Context contextSingle = metricRegistry().timer(name(PostProcessingTask.class, "execution-single")).time();
                try {
                    //Merge duplicate attributes if there are any
                    fixDuplicateAttributes(keyspace, logs, fixingRetries);

                    //Merge Duplicate Role Players if there are any
                    fixDuplicateRolePlayers(keyspace, logs, fixingRetries);
                } finally {
                    contextSingle.stop();
                }
            }
            return true;
        }
    }

    private void fixDuplicateRolePlayers(Keyspace keyspace, Json logs, int retries) {
        Set<ConceptId> relationshipIds = extractRolePlayerMergingJobs(Schema.BaseType.RELATIONSHIP, logs);

        relationshipIds.forEach(relationshipId ->{
            GraknTxMutators.runBatchMutationWithRetry(factory(), keyspace, retries, (tx) -> mergeDuplicateRolePlayers(tx, relationshipId));
        });
    }

    private void mergeDuplicateRolePlayers(GraknTx tx, ConceptId relationshipId) {
        if(tx.admin().relationshipHasDuplicateRolePlayers(relationshipId)){

            // Acquire a lock to make sure we don't have a race condition on deleting duplicate role players
            // and accidentally delete to many.
            Lock indexLock = this.getLockProvider().getLock(PostProcessingTask.RELATIONSHIP_LOCK_KEY + "/" + relationshipId.getValue());
            indexLock.lock();

            try {
                boolean commitNeeded = tx.admin().fixRelationshipWithDuplicateRolePlayers(relationshipId);

                if(commitNeeded){
                    tx.admin().commitNoLogs();
                }
            } finally {
                indexLock.unlock();
            }
        }
    }

    /**
     * Gets the list of {@link ai.grakn.concept.Relationship}s which have recently gotten new role players
     */
    private Set<ConceptId> extractRolePlayerMergingJobs(Schema.BaseType type, Json logs) {
        if(!logs.has(type.name())){
            return Collections.emptySet();
        }

        logs = logs.at(type.name());
        return logs.asList().stream().map(conceptId -> ConceptId.of((String) conceptId)).collect(Collectors.toSet());
    }

    private void fixDuplicateAttributes(Keyspace keyspace, Json logs, int retries){
        Map<String, Set<ConceptId>> attributesToMerge = extractAttributeMergingJobs(Schema.BaseType.ATTRIBUTE, logs);

        attributesToMerge.forEach((conceptIndex, conceptIds) -> {
            GraknTxMutators.runMutationWithRetry(factory(), keyspace, retries,
                    (tx) -> mergeDuplicateAttributes(tx, conceptIndex, conceptIds));
        });

        LOG.debug(JOB_FINISHED, Schema.BaseType.ATTRIBUTE.name(), attributesToMerge);
    }

    /**
     * Gets a map of indices to ids representing new {@link ai.grakn.concept.Attribute}s.
     * Using this map we can merge any potential duplicate attributes
     *
     * @return Map of concept indices to ids that has been extracted from the provided configuration.
     */
    private static Map<String,Set<ConceptId>> extractAttributeMergingJobs(Schema.BaseType type, Json logs) {
        if(!logs.has(type.name())){
            return Collections.emptyMap();
        }

        logs = logs.at(type.name());
        return logs.asJsonMap().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().asList().stream().map(o -> ConceptId.of(o.toString())).collect(Collectors.toSet())
        ));
    }

    /**
     * Apply the given post processing method to the provided concept index and set of ids.
     */
    private void mergeDuplicateAttributes(GraknTx tx, String conceptIndex, Set<ConceptId> conceptIds){
        if(tx.admin().duplicateResourcesExist(conceptIndex, conceptIds)){

            // Acquire a lock when you post process on an index to prevent race conditions
            // Lock is acquired after checking for duplicates to reduce runtime
            Lock indexLock = this.getLockProvider().getLock(PostProcessingTask.ATTRIBUTE_LOCK_KEY + "/" + conceptIndex);
            indexLock.lock();

            try {
                // execute the provided post processing method
                boolean commitNeeded = tx.admin().fixDuplicateResources(conceptIndex, conceptIds);

                // ensure post processing was correctly executed
                if(commitNeeded) {
                    validateMerged(tx, conceptIndex, conceptIds).
                            ifPresent(message -> {
                                throw new RuntimeException(message);
                            });

                    // persist merged concepts
                    tx.admin().commitNoLogs();
                }
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
    private Optional<String> validateMerged(GraknTx graph, String conceptIndex, Set<ConceptId> conceptIds){
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
        if(graph.admin().getConcept(Schema.VertexProperty.INDEX, conceptIndex) == null){
            return Optional.of("The concept index [" + conceptIndex + "] did not return any concept");
        }

        return Optional.empty();
    }

    /**
     * Helper method which creates PP Task States.
     *
     * @param creator The class which is creating the task
     * @return The executable postprocessing task state
     */
    public static TaskState createTask(Class creator, int delay) {
        return TaskState.of(PostProcessingTask.class,
                creator.getName(),
                TaskSchedule.at(Instant.now().plusMillis(delay)),
                TaskState.Priority.LOW);
    }

    /**
     * Helper method which creates the task config needed in order to execute a PP task
     *
     * @param keyspace The keyspace of the graph to execute this on.
     * @param config The config which contains the concepts to post process
     * @return The task configuration encapsulating the above details in a manner executable by the task runner
     */
    public static Optional<TaskConfiguration> createConfig(Keyspace keyspace, String config){
        Json jsonConfig = Json.read(config);
        if(!jsonConfig.has(REST.Request.COMMIT_LOG_FIXING)){
            return Optional.empty();
        }

        Json postProcessingConfiguration = Json.object();
        postProcessingConfiguration.set(REST.Request.KEYSPACE, keyspace.getValue());
        postProcessingConfiguration.set(REST.Request.COMMIT_LOG_FIXING, Json.read(config).at(REST.Request.COMMIT_LOG_FIXING));
        return Optional.of(TaskConfiguration.of(postProcessingConfiguration));
    }
}