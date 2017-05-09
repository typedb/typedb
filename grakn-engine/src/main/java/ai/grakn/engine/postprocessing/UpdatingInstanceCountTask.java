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

import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import java.util.stream.Collectors;

import java.util.Map;
import java.util.function.Consumer;

import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_INSTANCE_COUNT;
import static ai.grakn.util.REST.Request.COMMIT_LOG_TYPE_NAME;

/**
 * <p>
 *     Task that controls when types are updated with their new instance counts
 * </p>
 *
 * <p>
 *     This task begins only if enough time has passed (configurable) since the last time a job was added.
 * </p>
 *
 * @author fppt
 */
public class UpdatingInstanceCountTask extends AbstractLockingTask {

    public static final String LOCK_KEY = "/updating-instance-count-lock";

    @Override
    protected String getLockingKey() {
        return LOCK_KEY;
    }

    @Override
    public boolean runLockingBackgroundTask(Consumer<TaskCheckpoint> saveCheckpoint, TaskConfiguration configuration) {
        Map<TypeLabel, Long> jobs = getJobsFromConfiguration(configuration);

        GraphMutators.runGraphMutationWithRetry(configuration, (graph) -> {
            graph.admin().updateTypeShards(jobs);
            graph.admin().commitNoLogs();
        });

        return true;
    }

    private Map<TypeLabel, Long> getJobsFromConfiguration(TaskConfiguration configuration){
        return  configuration.json().at(COMMIT_LOG_COUNTING).asJsonList().stream()
                .collect(Collectors.toMap(
                        e -> TypeLabel.of(e.at(COMMIT_LOG_TYPE_NAME).asString()),
                        e -> e.at(COMMIT_LOG_INSTANCE_COUNT).asLong()));
    }
}