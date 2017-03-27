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

package ai.grakn.engine.tasks.mock;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMultiset;
import mjson.Json;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main task Mock class- keeps track of completed and failed tasks
 *
 * @author alexandraorth, Felix Chapman
 */
public abstract class MockBackgroundTask implements BackgroundTask {

    private final static Logger LOG = LoggerFactory.getLogger(SingleQueueTaskManager.class);

    private static final ConcurrentHashMultiset<TaskId> COMPLETED_TASKS = ConcurrentHashMultiset.create();
    private static final ConcurrentHashMultiset<TaskId> CANCELLED_TASKS = ConcurrentHashMultiset.create();
    private static Consumer<TaskId> onTaskStart;
    private static Consumer<TaskId> onTaskFinish;
    private static Consumer<TaskCheckpoint> onTaskResume;

    protected final AtomicBoolean cancelled = new AtomicBoolean(false);
    protected final Object sync = new Object();

    private static void addCompletedTask(TaskId taskId) {
        COMPLETED_TASKS.add(taskId);
    }

    public static ImmutableMultiset<TaskId> completedTasks() {
        return ImmutableMultiset.copyOf(COMPLETED_TASKS);
    }

    private static void addCancelledTask(TaskId taskId) {
        CANCELLED_TASKS.add(taskId);
    }

    public static ImmutableMultiset<TaskId> cancelledTasks() {
        return ImmutableMultiset.copyOf(CANCELLED_TASKS);
    }

    public static void whenTaskStarts(Consumer<TaskId> beforeTaskStarts) {
        MockBackgroundTask.onTaskStart = beforeTaskStarts;
    }

    private static void onTaskStart(TaskId taskId) {
        if (onTaskStart != null) onTaskStart.accept(taskId);
    }

    public static void whenTaskFinishes(Consumer<TaskId> onTaskFinish) {
        MockBackgroundTask.onTaskFinish = onTaskFinish;
    }

    private static void onTaskFinish(TaskId taskId) {
        if (onTaskFinish != null) onTaskFinish.accept(taskId);
    }

    public static void whenTaskResumes(Consumer<TaskCheckpoint> onTaskResume) {
        MockBackgroundTask.onTaskResume = onTaskResume;
    }

    private static void onTaskResume(TaskCheckpoint checkpoint) {
        if (onTaskResume != null) onTaskResume.accept(checkpoint);
    }

    public static void clearTasks() {
        COMPLETED_TASKS.clear();
        CANCELLED_TASKS.clear();
        onTaskStart = null;
        onTaskFinish = null;
        onTaskResume = null;
    }

    private TaskId id;

    @Override
    public final boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        id = TaskId.of(configuration.at("id").asString());
        onTaskStart(id);

        saveCheckpoint.accept(TaskCheckpoint.of(configuration));

        boolean wasCancelled = cancelled.get();

        if (!wasCancelled) {
            executeStartInner(id);
        }

        // Cancelled status may have changed
        wasCancelled = cancelled.get();

        if (!wasCancelled) {
            addCompletedTask(id);
        } else {
            addCancelledTask(id);
        }

        onTaskFinish(id);

        return !wasCancelled;
    }

    @Override
    public final boolean stop() {
        LOG.debug("Stopping {}", id);

        cancelled.set(true);
        synchronized (sync) {
            sync.notifyAll();
        }
        return true;
    }

    @Override
    public final boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint){
        onTaskResume(lastCheckpoint);

        executeResumeInner(lastCheckpoint);

        return true;
    }

    protected abstract void executeStartInner(TaskId id);
    protected abstract void executeResumeInner(TaskCheckpoint checkpoint);


}
