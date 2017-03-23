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
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.EngineContext;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FailoverElectorTest {

    private ZookeeperConnection zookeeper;
    private TaskStateStorage storage;

    @ClassRule
    public static final EngineContext kafka = EngineContext.startKafkaServer();

    @Before
    public void setupZkConnection() {
        zookeeper = new ZookeeperConnection();
        storage = new TaskStateInMemoryStore();
    }

    @After
    public void shutdownZkConnection() {
        zookeeper.close();
    }

    @Test
    public void whenFailoverElectorIsInstantiated_ThisEngineBecomesLeader(){
        EngineID engineId = EngineID.of("Engine1");
        FailoverElector elector = new FailoverElector(engineId, zookeeper, storage);
        assertEquals(engineId, elector.awaitLeader());
        elector.renounce();
    }

    @Test
    public void whenFailoverElectorIsKilled_AnotherFailoverElectorBecomesLeader() throws Exception {
        EngineID engine1 = EngineID.of("Engine1");
        EngineID engine2 = EngineID.of("Engine2");

        FailoverElector elector1 = new FailoverElector(engine1, zookeeper, storage);
        FailoverElector elector2 = new FailoverElector(engine2, zookeeper, storage);

        EngineID leader1 = elector1.awaitLeader();
        EngineID leader2 = elector2.awaitLeader();

        assertEquals(leader1, leader2);

        if(Objects.equals(leader1, engine1)){
            elector1.renounce();
        } else {
            elector2.renounce();
        }

        Thread.sleep(1000);

        System.out.println("whale");
        EngineID currentLeader;
        if(Objects.equals(leader1, engine1)) {
            currentLeader = elector2.awaitLeader();
        } else {
            currentLeader = elector1.awaitLeader();
        }

        System.out.println("starfish");
        assertNotEquals(leader1, currentLeader);

        System.out.println("should renounce soon");
        if(Objects.equals(leader1, engine1)) {
            elector2.renounce();
        } else {
            elector1.renounce();
        }
    }
}
