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
 *
 */

package ai.grakn.generator;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.mock.FailingMockTask;
import ai.grakn.engine.tasks.mock.LongExecutionMockTask;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import com.google.common.collect.ImmutableSet;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import mjson.Json;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.time.Instant;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class TaskStates extends Generator<TaskState> {

    private boolean newTask = false;

    // TODO: make this generate more classes
    @SuppressWarnings("unchecked")
    private Class<? extends BackgroundTask>[] classes = new Class[] {
            LongExecutionMockTask.class, ShortExecutionMockTask.class, FailingMockTask.class
    };

    public TaskStates() {
        super(TaskState.class);
    }

    @Override
    public TaskState generate(SourceOfRandomness random, GenerationStatus status) {
        Class<? extends BackgroundTask> taskClass = random.choose(classes);

        TaskId taskId;

        if (newTask) {
            taskId = TaskId.generate();
        } else {
            taskId = TaskId.of(random.choose(ImmutableSet.of("A", "B", "C")));
        }

        // TODO: Make this generate random task statuses

        String creator = gen().type(String.class).generate(random, status);

        // TODO: generate all the other params of a task state

        // A bit in the past, because Instant is not monotonic
        TaskSchedule schedule = TaskSchedule.at(Instant.now().minusSeconds(60));

        Json configuration = Json.object();
        TaskState taskState = TaskState.of(taskClass, creator, schedule, configuration, taskId);
        configuration.set("id", taskState.getId().getValue());
        return taskState;
    }

    public void configure(NewTask newTask) {
        this.newTask = newTask.value();
    }

    public void configure(WithClass withClass) {
        this.classes = withClass.value();
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface NewTask {
        boolean value() default true;
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface WithClass {
        Class<? extends BackgroundTask>[] value() default {};
    }
}
