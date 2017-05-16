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

package ai.grakn.engine.postprocessing.util;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     Helper class to extract details from commit log
 * </p>
 *
 * <p>
 *     This class is used to extract information from Task configs which is needed by
 *     {@link ai.grakn.engine.postprocessing.PostProcessingTask},
 *     {@link ai.grakn.engine.postprocessing.UpdatingInstanceCountTask} and
 *     {@link ai.grakn.engine.loader.LoaderTask}
 * </p>
 *
 * @author fppt
 */
public class TaskConfigReader {

    /**
     * Extracts the keyspace of the graph which sent the commit log
     *
     * @param config the json file
     * @return the keyspace of the graph which was mutated
     */
    public static String getKeyspace(TaskConfiguration config){
        return config.json().at(REST.Request.KEYSPACE).asString();
    }

    /**
     * Extracts the type labels and count from the Json configuration
     * @param configuration The configuration which contains types counts
     * @return A map indicating the number of instances each type has gained or lost
     */
    public static Map<TypeLabel, Long> getCountUpdatingJobs(TaskConfiguration configuration){
        return  configuration.json().at(REST.Request.COMMIT_LOG_COUNTING).asJsonList().stream()
                .collect(Collectors.toMap(
                        e -> TypeLabel.of(e.at(REST.Request.COMMIT_LOG_CONCEPT_ID).asString()),
                        e -> e.at(REST.Request.COMMIT_LOG_SHARDING_COUNT).asLong()));
    }

    /**
     * Extract a map of concept indices to concept ids from the provided configuration
     *
     * @param type Type of concept to extract. This correlates to the key in the provided configuration.
     * @param configuration Configuration from which to extract the configuration.
     * @return Map of concept indices to ids that has been extracted from the provided configuration.
     */
    public static Map<String,Set<ConceptId>> getPostProcessingJobs(Schema.BaseType type, TaskConfiguration configuration) {
        return configuration.json().at(REST.Request.COMMIT_LOG_FIXING).at(type.name()).asJsonMap().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().asList().stream().map(ConceptId::of).collect(Collectors.toSet())
        ));
    }
}
