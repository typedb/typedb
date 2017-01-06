/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.engine.backgroundtasks.config;


public interface ZookeeperPaths {
    String TASKS_NAMESPACE = "grakn";
    String SCHEDULER = "/scheduler";
    String TASK_RUNNERS = "/task_runners";
    String RUNNERS_WATCH = TASK_RUNNERS+"/watch";
    String RUNNERS_STATE = TASK_RUNNERS+"/last_state";
    String TASKS_PATH_PREFIX = "/tasks";
    String TASK_STATE_SUFFIX = "/state";
    String TASK_LOCK_SUFFIX = "/lock";
}
