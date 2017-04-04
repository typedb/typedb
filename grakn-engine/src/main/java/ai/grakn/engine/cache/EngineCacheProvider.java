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

package ai.grakn.engine.cache;

import ai.grakn.graph.admin.ConceptCache;

/**
 * <p>
 *     Provides the correct engine cache
 * </p>
 *
 * <p>
 *     This class provides the correct {@link ai.grakn.graph.admin.ConceptCache} based on how it was initliased.
 *
 *     If initialised by {@link ai.grakn.engine.tasks.manager.StandaloneTaskManager} then it provides the in memory
 *     cache {@link EngineCacheStandAlone}.
 *
 *     If initialised by {@link ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager} then it provides
 *     the distributed cache {@link EngineCacheDistributed} which persists the cache inside zookeeper
 * </p>
 *
 * @author fppt
 */
public class EngineCacheProvider {
    private ConceptCache conceptCache;
    private static EngineCacheProvider instance=null;

    public EngineCacheProvider (ConceptCache conceptCache){
        this.conceptCache = conceptCache;
    }

    public static synchronized EngineCacheProvider getInstance(){
        if(instance==null) throw new RuntimeException("Cache has not been initialised");
        return instance;
    }
}
