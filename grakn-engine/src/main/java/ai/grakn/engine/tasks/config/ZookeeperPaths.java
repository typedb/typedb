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

package ai.grakn.engine.tasks.config;

/**
 * <p>
 * Class containing strings that describe the file storage locations in Zookeeper
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 * */
//TODO FIX THIS WHOLE CLASS IT IS AWFUL
public interface ZookeeperPaths {
    String TASKS_NAMESPACE = "grakn";
    String SCHEDULER = "/scheduler";
    String FAILOVER = "/failover";
    String TASKS_PATH_PREFIX = "/tasks";
    String TASKS_STOPPED_PREFIX = "/stopped";
    String TASKS_STOPPED = "/stopped/%s";
    String TASK_LOCK_SUFFIX = "/lock";
    String PARTITION_PATH = "/partition/%s";
    String ALL_ENGINE_PATH = "/engine";
    String ALL_ENGINE_WATCH_PATH = "/engine/watch";
    String SINGLE_ENGINE_WATCH_PATH = "/engine/watch/%s";
    String SINGLE_ENGINE_PATH = ALL_ENGINE_PATH + "/%s";
    String ZK_TASK_PATH =  TASKS_PATH_PREFIX + "/%s";
    String ZK_ENGINE_TASK_PATH = ALL_ENGINE_PATH + "/%s/%s";

    String ENGINE_CACHE = "/engine/cache/";
    String ENGINE_CACHE_KEYSPACES = ENGINE_CACHE + "keyspaces";

    String ENGINE_CACHE_UPDATE_TIME = ENGINE_CACHE + "last-update";
    String ENGINE_CACHE_JOB_TYPE = ENGINE_CACHE + "%s/%s"; //Used to get all the indices of a job type
    String ENGINE_CACHE_CONCEPT_IDS = ENGINE_CACHE_JOB_TYPE +  "/%s"; //Used to get all the ids of a specific index
    String ENGINE_CACHE_EXACT_JOB = ENGINE_CACHE_CONCEPT_IDS + "/%s";
    String ENGINE_CACHE_TYPE_INSTANCE_COUNT = ENGINE_CACHE_JOB_TYPE + "/%s";
}
