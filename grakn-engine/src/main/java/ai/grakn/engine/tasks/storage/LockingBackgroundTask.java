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

package ai.grakn.engine.tasks.storage;

import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import mjson.Json;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * <p>
 *     Task that control whens locking tasks begin
 * </p>
 *
 * <p>
 *     This utility class is used to help with background tasks which need to acquire a lock before starting
 * </p>
 *
 * @author alexandraorth, fppt
 */
public abstract class LockingBackgroundTask implements BackgroundTask{

    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        Lock engineLock = LockProvider.getLock(getLockingKey());
        try {
            // Try to get lock for one second. If task cannot acquire lock, it should return successfully.
            boolean hasLock = engineLock.tryLock(1, TimeUnit.SECONDS);

            // If you have the lock, run the job and then release the lock
            if (hasLock) {
                try {
                    return runLockingBackgroundTask(saveCheckpoint, configuration);
                } finally {
                    engineLock.unlock();
                }
            }
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }

        return true;
    }

    /**
     *
     * @return The key used to acquire the lock needed before running this job
     */
    protected abstract String getLockingKey();

    /**
     *
     * @return The actual job to run once the job is acquired
     */
    protected abstract boolean runLockingBackgroundTask(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration);
}
