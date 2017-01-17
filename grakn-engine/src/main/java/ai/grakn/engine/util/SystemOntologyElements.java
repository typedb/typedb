package ai.grakn.engine.util;

import ai.grakn.concept.TypeName;

public interface SystemOntologyElements {
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
}
