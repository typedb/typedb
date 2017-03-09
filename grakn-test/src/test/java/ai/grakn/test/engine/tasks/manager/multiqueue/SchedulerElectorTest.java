/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.tasks.manager.multiqueue;

import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.multiqueue.Scheduler;
import ai.grakn.engine.tasks.manager.multiqueue.SchedulerElector;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.test.EngineContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Predicate;

import static org.junit.Assert.assertNotEquals;

public class SchedulerElectorTest {
    private static SchedulerElector elector;
    private static ZookeeperConnection connection;

    @ClassRule
    public static final EngineContext engine = EngineContext.startKafkaServer();

    @BeforeClass
    public static void instantiate(){
        connection = new ZookeeperConnection();
        elector = new SchedulerElector(new TaskStateInMemoryStore(), connection);
    }

    @AfterClass
    public static void teardown(){
        elector.stop();
        connection.close();
    }

    @Test
    public void testSchedulerRestartsAfterKilled() throws Exception {
        waitForScheduler(Objects::nonNull);
        Scheduler scheduler1 = elector.getScheduler();

        // Kill scheduler- client should create a new one
        scheduler1.close();

        waitForScheduler(Objects::isNull);

        Scheduler scheduler2 = elector.getScheduler();
        assertNotEquals(scheduler1, scheduler2);
    }

    protected void waitForScheduler(Predicate<Scheduler> fn) throws Exception {
        int runs = 0;

        while (!fn.test(elector.getScheduler()) && runs < 50 ) {
            Thread.sleep(100);
            runs++;
        }

        System.out.println("wait done, runs " + runs + " scheduler " + elector.getScheduler());
    }
}
