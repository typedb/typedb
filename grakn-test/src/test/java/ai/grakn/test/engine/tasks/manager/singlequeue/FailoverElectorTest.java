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

package ai.grakn.test.engine.tasks.manager.singlequeue;

import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.engine.tasks.manager.singlequeue.FailoverElector;
import ai.grakn.engine.tasks.storage.TaskStateInMemoryStore;
import ai.grakn.test.EngineContext;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Objects;

import static ai.grakn.engine.tasks.config.ConfigHelper.client;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FailoverElectorTest {

    private ZookeeperConnection zookeeper;
    private TaskStateStorage storage;

    @ClassRule
    public static final EngineContext kafka = EngineContext.startKafkaServer();

    @Before
    public void setupZkConnection() {
        zookeeper = new ZookeeperConnection(client());
        storage = new TaskStateInMemoryStore();
    }

    @After
    public void shutdownZkConnection() {
        zookeeper.close();
    }

    @Test
    public void whenFailoverElectorIsInstantiated_ThisEngineBecomesLeader(){
        FailoverElector elector = new FailoverElector("Engine1", zookeeper, storage);
        assertEquals("Engine1", elector.awaitLeader());
        elector.stop();
    }

    @Test
    public void whenFailoverElectorIsKilled_AnotherFailoverElectorBecomesLeader() throws Exception {
        FailoverElector elector1 = new FailoverElector("Engine1", zookeeper, storage);
        FailoverElector elector2 = new FailoverElector("Engine2", zookeeper, storage);

        assertEquals(elector1.awaitLeader(), elector2.awaitLeader());

        String currentLeader = elector1.awaitLeader();
        if(Objects.equals(currentLeader, "Engine1")){
            elector1.stop();
        } else {
            elector2.stop();
        }

        Thread.sleep(1000);

        assertEquals(elector1.awaitLeader(), elector2.awaitLeader());
        assertNotEquals(currentLeader, elector1.awaitLeader());

        elector1.stop();
        elector2.stop();
    }

}
