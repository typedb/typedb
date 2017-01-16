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

package ai.grakn.engine.util;

public interface SystemOntologyElements {
    String SCHEDULED_TASK = "scheduled-task";
    String STATUS = "status";
    String STATUS_CHANGE_TIME = "status-change-time";
    String STATUS_CHANGE_BY = "status-change-by";
    String TASK_CLASS_NAME = "task-class-name";
    String CREATED_BY = "created-by";
    String ENGINE_ID = "engine-id";
    String RUN_AT = "run-at";
    String RECURRING = "recurring";
    String RECUR_INTERVAL = "recur-interval";
    String STACK_TRACE = "stack-trace";
    String TASK_EXCEPTION = "task-exception";
    String TASK_CHECKPOINT = "task-checkpoint";
    String TASK_CONFIGURATION = "task-configuration";
}
