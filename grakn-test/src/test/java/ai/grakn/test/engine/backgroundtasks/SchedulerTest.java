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
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.backgroundtasks.distributed.KafkaLogger;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import javafx.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.CREATED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.NEW_TASKS_TOPIC;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNotNull;

/**
 * Each test needs to be run with a clean Kafka to pass
 */
public class SchedulerTest {
    private GraknStateStorage stateStorage = new GraknStateStorage();

    @Rule
    public final EngineContext engine = EngineContext.startServer();

    @BeforeClass
    public static void setup(){
        ((Logger) org.slf4j.LoggerFactory.getLogger(KafkaLogger.class)).setLevel(Level.DEBUG);
    }

    @Test
    public void testInstantaneousOneTimeTasks() throws Exception {
        Map<String, TaskState> tasks = createTasks(5);
        sendTasksToNewTasksQueue(tasks);
        waitUntilScheduled(tasks.keySet());
    }

    private Map<String, TaskState> createTasks(int n) throws Exception {
        Map<String, TaskState> tasks = new HashMap<>();

        for(int i=0; i < n; i++) {
            Pair<String, TaskState> task = createTask(i, CREATED, false, 0);
            tasks.put(task.getKey(), task.getValue());

            System.out.println("task " + i + " created");
        }

        return tasks;
    }

    private Pair<String, TaskState> createTask(int i, TaskStatus status, boolean recurring, int interval) throws Exception {
        String taskId = stateStorage.newState(
                TestTask.class.getName(),
                SchedulerTest.class.getName(),
                Instant.now(), recurring, interval, new JSONObject(singletonMap("name", "task"+i)));

        stateStorage.updateState(taskId, status, null, null, null, null, null);

        TaskState state = stateStorage.getState(taskId);

        engine.getClusterManager().getStorage().newState(taskId, status, null, null);

        assertNotNull(taskId);
        assertNotNull(state);

        return new Pair<>(taskId, state);
    }

    private void sendTasksToNewTasksQueue(Map<String, TaskState> tasks) {
        KafkaProducer<String, String> producer = ConfigHelper.kafkaProducer();

        for(String taskId:tasks.keySet()){
            producer.send(new ProducerRecord<>(NEW_TASKS_TOPIC, taskId, tasks.get(taskId).configuration().toString()));
        }

        producer.flush();
        producer.close();
    }

    private void waitUntilScheduled(Collection<String> tasks) {
        tasks.forEach(this::waitUntilScheduled);
    }

    private void waitUntilScheduled(String taskId) {
        final long initial = new Date().getTime();

        while((new Date().getTime())-initial < 60000) {
            TaskStatus status = engine.getClusterManager().getStorage().getState(taskId).status();
            System.out.println(taskId + "  -->>  " + status);
            if(status == SCHEDULED || status == RUNNING || status == COMPLETED) {
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            }

            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
