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

package ai.grakn.engine.postprocessing;

import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import mjson.Json;

import java.util.function.Consumer;

/**
 * <p>
 *     Task that controls when types are updated with their new instance counts
 * </p>
 *
 * <p>
 *     This task begins only if enough time has passed (configurable) since the last time a job was added.
 * </p>
 *
 * @author fppt
 */
public class UpdatingInstanceCountTask implements BackgroundTask {
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
