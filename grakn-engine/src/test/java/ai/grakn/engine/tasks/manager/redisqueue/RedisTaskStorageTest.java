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

package ai.grakn.engine.tasks.manager.redisqueue;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.redisq.Redisq;
import ai.grakn.redisq.State;
import ai.grakn.redisq.StateInfo;
import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import java.util.Optional;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedisTaskStorageTest {

    private static Redisq<Task> redisq = mock(Redisq.class);
    private static MetricRegistry metric = mock(MetricRegistry.class);
    private static RedisTaskStorage redisTaskStorage = RedisTaskStorage.create(redisq, metric);

    @Test
    public void whenRedisReturnsFailedTask_EnsureExceptionIsReturned(){
        //Create a fake failing task and fail it
        String myException = "this task smelt funny";
        StateInfo info = new StateInfo(State.FAILED, 0L, myException);
        TaskId taskId = TaskId.generate();

        //Make sure the fake task is returned
        when(redisq.getState(taskId.getValue())).thenReturn(Optional.of(info));

        //Check the exception is preserved
        TaskState state = redisTaskStorage.getState(taskId);
        assertEquals(TaskStatus.FAILED, state.status());
        assertNotNull(state.exception());
        assertEquals(myException, state.exception());
    }
}
