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
import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.storage.LockingBackgroundTask;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.util.ErrorMessage;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.COMMIT_LOG_INSTANCE_COUNT;
import static ai.grakn.util.REST.Request.COMMIT_LOG_TYPE_NAME;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static java.util.stream.Collectors.toMap;

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
public class UpdatingInstanceCountTask extends LockingBackgroundTask {
    public static final String LOCK_KEY = "updating-instance-count-lock";
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.LOG_NAME_POSTPROCESSING_DEFAULT);

    @Override
    protected String getLockingKey() {
        return LOCK_KEY;
    }

    @Override
    protected boolean runLockingBackgroundTask(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        String keyspace = configuration.at(KEYSPACE).asString();
        Json instancesToCount = configuration.at(COMMIT_LOG_COUNTING);

        Map<TypeLabel, Long> instanceMap = instancesToCount
                .asJsonList().stream()
                .collect(toMap(
                        e -> TypeLabel.of(e.at(COMMIT_LOG_TYPE_NAME).asString()),
                        e -> e.at(COMMIT_LOG_INSTANCE_COUNT).asLong()));

        updateCountsOnKeySpace(keyspace, instanceMap);

        return false;
    }

    private void updateCountsOnKeySpace(String keyspace, Map<TypeLabel, Long> jobs){
        //TODO: All this boiler plate retry should be moved into a common graph mutating background task

        boolean notDone = true;
        int retry = 0;

        while(notDone) {
            notDone = false;
            try (GraknGraph graknGraph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE)) {
                graknGraph.admin().updateTypeShards(jobs);
                graknGraph.admin().commitNoLogs();
            } catch (Throwable e) {
                LOG.warn("Unable to updating instance counts of graph [" + keyspace + "]", e);
                if(retry > 10){
                    //TODO Resubmit this task somehow, so as not to lose counts
                    throw new RuntimeException("Failed 10 times in a row to update the counts of the types on graph [" + keyspace + "] giving up");
                } else {
                    retry = performRetry(retry);
                    notDone = true;
                }
            }
        }
    }

    private static int performRetry(int retry){
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

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
