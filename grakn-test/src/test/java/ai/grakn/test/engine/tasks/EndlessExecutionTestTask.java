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

import ai.grakn.engine.tasks.TaskId;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.addCancelledTask;

public class EndlessExecutionTestTask extends MockBackgroundTask {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final Object sync = new Object();

    @Override
    protected boolean startInner(TaskId id) {
        // Never return until stopped
        if (!cancelled.get()) {
            synchronized (sync) {
                try {
                    sync.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        addCancelledTask(id);
        return false;
    }

    public boolean stop() {
        cancelled.set(true);
        synchronized (sync) {
            sync.notify();
        }
        return true;
    }

    public void pause() {}

    public void resume(Consumer<String> c, String s) {}
}
