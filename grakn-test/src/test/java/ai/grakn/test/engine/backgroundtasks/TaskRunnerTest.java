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

import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.test.EngineTestBase;
import javafx.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.*;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static java.util.Collections.singletonMap;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assume.assumeFalse;
import static ai.grakn.test.GraknTestEnv.*;

public class TaskRunnerTest extends EngineTestBase {
    private KafkaProducer<String, String> producer;
    private StateStorage stateStorage;
    private SynchronizedStateStorage zkStorage;

    @BeforeClass
    public static void startEngine() throws Exception{
        ((Logger) org.slf4j.LoggerFactory.getLogger(KafkaLogger.class)).setLevel(Level.DEBUG);
    }

    @Before
    public void setup() throws Exception {
        producer = ConfigHelper.kafkaProducer();
        stateStorage = new GraknStateStorage();

        // ZooKeeper client
        zkStorage = SynchronizedStateStorage.getInstance();
        assumeFalse(usingTinker());
    }

    @After
    public void tearDown() throws Exception {
        producer.close();
    }

    @Test
    public void testSendReceive() throws Exception {
        TestTask.startedCounter.set(0);
        precomputeStates(5, SCHEDULED).forEach(x -> {
            producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, x.getKey(), x.getValue().configuration().toString()));
            System.out.println("Task to work queue - " + x.getKey());
        });
        producer.flush();

        waitUntilXTasksFinishedOrTimeout(5);
        assertEquals(5, TestTask.startedCounter.get());
    }

    @Test
    public void testSendDuplicate() throws Exception {
        TestTask.startedCounter.set(0);
        precomputeStates(5, SCHEDULED).forEach(x -> {
            producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, x.getKey(), x.getValue().configuration().toString()));
            producer.send(new ProducerRecord<>(WORK_QUEUE_TOPIC, x.getKey(), x.getValue().configuration().toString()));
            System.out.println("Task to work queue - " + x.getKey());
        });
        producer.flush();

        waitUntilXTasksFinishedOrTimeout(5);
        assertEquals(5, TestTask.startedCounter.get());
    }

    /**
     * Precompute states so that they can later be sent quickly
     */
    private Collection<Pair<String, TaskState>> precomputeStates(int count, TaskStatus status) throws Exception {
        Collection<Pair<String, TaskState>> states = new HashSet<>();

        for (int i = 0; i < count; i++) {
            String id = stateStorage.newState(TestTask.class.getName(),
                        this.getClass().getName(),
                        new Date(), false, 0,
                        new JSONObject(singletonMap("name", "task "+i)));

            TaskState state = stateStorage.getState(id);
            state.status(status);
            zkStorage.newState(id, state.status(), null, null);

            states.add(new Pair<>(id, state));
        }

        return states;
    }

    private void waitUntilXTasksFinishedOrTimeout(int numberShouldFinish) throws InterruptedException{
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 60000) {
            if (TestTask.startedCounter.get() == numberShouldFinish) {
                break;
            }

            Thread.sleep(1000);
        }

    }
}
