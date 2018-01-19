/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine.task;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Helper class which wraps a {@link java.util.concurrent.ScheduledExecutorService} which runs periodically
 *     and executes {@link BackgroundTask}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class BackgroundTaskRunner {
    private final ScheduledExecutorService threadPool;

    public BackgroundTaskRunner(GraknConfig graknConfig) {
        int numThread = graknConfig.getProperty(GraknConfigKey.NUM_BACKGROUND_THREADS);
        threadPool = Executors.newScheduledThreadPool(numThread);
    }

    /**
     *
     * @param backgroundTask
     */
    public void submit(BackgroundTask backgroundTask){
        threadPool.scheduleAtFixedRate(backgroundTask::run, backgroundTask.period(), backgroundTask.period(), TimeUnit.MINUTES);
    }
}
