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

import ai.grakn.engine.tasks.TaskCheckpoint;
import java.time.Duration;

/**
 * Mocked task that will never end
 *
 * @author alexandraorth, Felix Chapman
 */
public class EndlessExecutionMockTask extends MockBackgroundTask {

    @Override
    protected void executeStartInner(TaskId id) {
        // Never return until stopped
        if (!cancelled.get()) {
            synchronized (sync) {
                try {
                    sync.wait(Duration.ofMinutes(5).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    protected void executeResumeInner(TaskCheckpoint checkpoint) {}

    public void pause() {}
}
