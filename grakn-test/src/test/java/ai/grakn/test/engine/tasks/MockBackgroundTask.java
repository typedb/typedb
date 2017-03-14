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

package ai.grakn.test.engine.tasks;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.BackgroundTask;
import mjson.Json;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.addCancelledTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.addCompletedTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.onTaskFinish;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.onTaskStart;

public abstract class MockBackgroundTask implements BackgroundTask {

    protected final AtomicBoolean cancelled = new AtomicBoolean(false);
    protected final Object sync = new Object();

    @Override
    public final boolean start(Consumer<String> saveCheckpoint, Json configuration) {
        TaskId id = TaskId.of(configuration.at("id").asString());
        onTaskStart(id);

        boolean wasCancelled = cancelled.get();

        if (!wasCancelled) {
            startInner(id);
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
        cancelled.set(true);
        synchronized (sync) {
            sync.notify();
        }
        return true;
    }

    protected abstract void startInner(TaskId id);
}
