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
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.util.ErrorMessage;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

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
public class UpdatingInstanceCountTask implements BackgroundTask {
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.LOG_NAME_POSTPROCESSING_DEFAULT);
    private ConceptCache cache = EngineCacheProvider.getCache();

    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        cache.getKeyspaces().parallelStream().forEach(this::updateCountsOnKeySpace);
        return true;
    }

    private void updateCountsOnKeySpace(String keyspace){
        Map<TypeLabel, Long> jobs = new HashMap<>(cache.getInstanceCountJobs(keyspace));
        //Clear the cache optimistically because we think we going to update successfully
        jobs.forEach((key, value) -> cache.deleteJobInstanceCount(keyspace, key));

        //TODO: All this boiler plate retry should be moved into a common graph mutating background task

        boolean notDone = true;
        int retry = 0;

        while(notDone) {
            notDone = false;
            try (GraknGraph graknGraph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE)) {
                graknGraph.admin().updateTypeCounts(jobs);
                graknGraph.admin().commitNoLogs();
            } catch (Throwable e) {
                LOG.error("Unable to updating instance counts of graph [" + keyspace + "]", e);
                if(retry > 10){
                    LOG.error("Failed 10 times in a row to update the counts of the types on graph [" + keyspace + "] giving up");
                    jobs.forEach((key, value) -> cache.addJobInstanceCount(keyspace, key, value));
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
