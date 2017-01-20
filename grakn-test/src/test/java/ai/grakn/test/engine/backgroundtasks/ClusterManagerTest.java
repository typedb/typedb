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

package ai.grakn.test.engine.backgroundtasks;

import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.test.EngineContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Predicate;

import static org.junit.Assert.assertNotEquals;

public class ClusterManagerTest {
    private static ClusterManager clusterManager;

    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @BeforeClass
    public static void instantiate(){
        clusterManager = engine.getClusterManager();
    }

    @Test
    public void testSchedulerRestartsAfterKilled() throws Exception {
        synchronized (clusterManager.getScheduler()) {
            waitForScheduler(clusterManager, Objects::nonNull);
            Scheduler scheduler1 = clusterManager.getScheduler();

            // Kill scheduler- client should create a new one
            scheduler1.close();

            waitForScheduler(clusterManager, Objects::nonNull);

            Scheduler scheduler2 = clusterManager.getScheduler();
            assertNotEquals(scheduler1, scheduler2);
        }
    }

    protected void waitForScheduler(ClusterManager clusterManager, Predicate<Scheduler> fn) throws Exception {
        int runs = 0;

        while (fn.test(clusterManager.getScheduler()) && runs < 50 ) {
            Thread.sleep(100);
            runs++;
        }

        System.out.println("wait done, runs " + runs + " scheduler " + clusterManager.getScheduler());
    }
}
