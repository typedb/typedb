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
import ai.grakn.concept.TypeName;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.admin.ConceptCache;
import mjson.Json;

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
    private ConceptCache cache = EngineCacheProvider.getCache();

    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        cache.getKeyspaces().parallelStream().forEach(this::updateCountsOnKeySpace);
        return true;
    }
    private void updateCountsOnKeySpace(String keyspace){
        Map<TypeName, Long> jobs = cache.getInstanceCountJobs(keyspace);
        //Clear the cache optimistically because we think we going to update successfully
        jobs.forEach((key, value) -> cache.deleteJobInstanceCount(keyspace, key));

        try(GraknGraph graknGraph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE)){
            graknGraph.admin().updateTypeCounts(jobs);
            graknGraph.admin().commitNoLogs();
        }
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
