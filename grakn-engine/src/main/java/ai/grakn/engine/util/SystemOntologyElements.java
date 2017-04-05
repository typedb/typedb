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

import ai.grakn.concept.TypeLabel;

/**
 * <p>
 *     Describes the system ontology elements related to task loading
 * </p>
 *
 * @author Denis Lobanov
 */
public interface SystemOntologyElements {
    TypeLabel TASK_ID = TypeLabel.of("task-id");
    TypeLabel SCHEDULED_TASK = TypeLabel.of("scheduled-task");
    TypeLabel STATUS = TypeLabel.of("status");
    TypeLabel STATUS_CHANGE_TIME = TypeLabel.of("status-change-time");
    TypeLabel STATUS_CHANGE_BY = TypeLabel.of("status-change-by");
    TypeLabel TASK_CLASS_NAME = TypeLabel.of("task-class-name");
    TypeLabel CREATED_BY = TypeLabel.of("created-by");
    TypeLabel ENGINE_ID = TypeLabel.of("engine-id");
    TypeLabel RUN_AT = TypeLabel.of("run-at");
    TypeLabel RECURRING = TypeLabel.of("recurring");
    TypeLabel RECUR_INTERVAL = TypeLabel.of("recur-interval");
    TypeLabel STACK_TRACE = TypeLabel.of("stack-trace");
    TypeLabel TASK_EXCEPTION = TypeLabel.of("task-exception");
    TypeLabel TASK_CHECKPOINT = TypeLabel.of("task-checkpoint");
    TypeLabel TASK_CONFIGURATION = TypeLabel.of("task-configuration");
    TypeLabel SERIALISED_TASK = TypeLabel.of("task-serialized");
}
