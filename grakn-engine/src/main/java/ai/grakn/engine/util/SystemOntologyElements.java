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

import ai.grakn.concept.TypeName;

/**
 * <p>
 *     Describes the system ontology elements related to task loading
 * </p>
 *
 * @author Denis Lobanov
 */
public interface SystemOntologyElements {
    TypeName TASK_ID = TypeName.of("task-id");
    TypeName SCHEDULED_TASK = TypeName.of("scheduled-task");
    TypeName STATUS = TypeName.of("status");
    TypeName STATUS_CHANGE_TIME = TypeName.of("status-change-time");
    TypeName STATUS_CHANGE_BY = TypeName.of("status-change-by");
    TypeName TASK_CLASS_NAME = TypeName.of("task-class-name");
    TypeName CREATED_BY = TypeName.of("created-by");
    TypeName ENGINE_ID = TypeName.of("engine-id");
    TypeName RUN_AT = TypeName.of("run-at");
    TypeName RECURRING = TypeName.of("recurring");
    TypeName RECUR_INTERVAL = TypeName.of("recur-interval");
    TypeName STACK_TRACE = TypeName.of("stack-trace");
    TypeName TASK_EXCEPTION = TypeName.of("task-exception");
    TypeName TASK_CHECKPOINT = TypeName.of("task-checkpoint");
    TypeName TASK_CONFIGURATION = TypeName.of("task-configuration");
    TypeName SERIALISED_TASK = TypeName.of("task-serialized");
}
