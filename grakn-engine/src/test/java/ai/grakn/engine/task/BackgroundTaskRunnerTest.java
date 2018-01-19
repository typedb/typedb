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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackgroundTaskRunnerTest {
    private static BackgroundTaskRunner taskRunner;

    @BeforeClass
    public static void configureMocks(){
        GraknConfig config = mock(GraknConfig.class);
        when(config.getProperty(GraknConfigKey.NUM_BACKGROUND_THREADS)).thenReturn(1);

        taskRunner = new BackgroundTaskRunner(config);
    }

    @AfterClass
    public static void close(){
        taskRunner.close();
    }

    @Test
    public void whenSubmittingBackgroundTask_EnsureItIsRunPeriodically() throws InterruptedException {
        //Create fake task
        BackgroundTask backgroundTask = mock(BackgroundTask.class);
        when(backgroundTask.period()).thenReturn(1);

        //Submit it
        taskRunner.register(backgroundTask);

        //Wait for 5 seconds
        Thread.sleep(5000);

        //Ensure the job has run a few times
        verify(backgroundTask, Mockito.atLeast(3)).run();
    }
}
