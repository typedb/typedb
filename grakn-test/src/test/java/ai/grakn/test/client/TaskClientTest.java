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

package ai.grakn.test.client;

import ai.grakn.Grakn;
import ai.grakn.client.TaskClient;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.TaskManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import spark.Service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TaskClientTest {

    private TaskClient client;
    private TaskManager manager;
    private Service spark;

    @Before
    public void setUp() {
        client = TaskClient.of(Grakn.DEFAULT_URI);

        spark = Service.ignite();
        spark.port(4567);

        manager = mock(TaskManager.class);

        new TasksController(spark, manager);

        spark.awaitInitialization();
    }

    @After
    public void tearDown() {
        spark.stop();
    }

    @Test
    public void whenStoppingATask_TheTaskManagerIsToldToStopTheTask() {
        TaskId taskId = TaskId.generate();

        client.stopTask(taskId);

        verify(manager).stopTask(eq(taskId), any());
    }
}