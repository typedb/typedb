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

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.EngineID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.CreateMode;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static ai.grakn.engine.backgroundtasks.TaskStatus.COMPLETED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.FAILED;
import static ai.grakn.engine.backgroundtasks.TaskStatus.RUNNING;
import static ai.grakn.engine.backgroundtasks.TaskStatus.SCHEDULED;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.TASK_RUNNER_GROUP;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;
import static ai.grakn.engine.util.ConfigProperties.TASKRUNNER_POLLING_FREQ;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

public class TaskRunner implements Runnable, AutoCloseable {
    private final static KafkaLogger LOG = KafkaLogger.getInstance();
    private final static ConfigProperties properties = ConfigProperties.getInstance();

    private final static int POLLING_FREQUENCY = properties.getPropertyAsInt(TASKRUNNER_POLLING_FREQ);
    private final static int EXECUTOR_SIZE = properties.getAvailableThreads();
    private final static String ENGINE_ID = EngineID.getInstance().id();

    private ExecutorService executor;
    private final Set<String> runningTasks = new HashSet<>();
    private final AtomicBoolean OPENED = new AtomicBoolean(false);

    private StateStorage graknStorage;
    private final SynchronizedStateStorage zkStorage;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running;
    private boolean initialised = false;

    public TaskRunner(SynchronizedStateStorage zkStorage) throws Exception {
        this.zkStorage = zkStorage;

        if(OPENED.compareAndSet(false, true)) {
            graknStorage = new GraknStateStorage();

            consumer = kafkaConsumer(TASK_RUNNER_GROUP);
            consumer.subscribe(singletonList(WORK_QUEUE_TOPIC), new RebalanceListener(consumer));

            // Create initial entries in ZK for TaskFailover to watch.
            registerAsRunning();
            updateOwnState();
            executor = Executors.newFixedThreadPool(properties.getAvailableThreads());

            LOG.info("TaskRunner opened.");
        }
        else {
            LOG.error("TaskRunner already opened!");
        }


        running = false;
    }

    /**
     * Start the main loop, this will block until a call to stop().
     */
    public void run()  {
        running = true;
        try {
            while (running) {
                printInitialization();
                LOG.debug("TaskRunner polling, size of new tasks " + consumer.endOffsets(consumer.partitionsFor(WORK_QUEUE_TOPIC).stream().map(i -> new TopicPartition(WORK_QUEUE_TOPIC, i.partition())).collect(toSet())));

                // Poll for new tasks only when we know we have space to accept them.
                if (getRunningTasksCount() < EXECUTOR_SIZE) {
                    processRecords(consumer.poll(POLLING_FREQUENCY));
                }
            }

        }
        catch (WakeupException e) {
            if (running) {
                LOG.error("TaskRunner interrupted unexpectedly (without clearing 'running' flag first", e);
            } else {
                LOG.debug("TaskRunner exiting gracefully.");
            }
        }
        finally {
            consumer.commitSync();
            consumer.close();
        }
    }

    /**
     * Stop the main loop, causing run() to exit.
     */
    public void close() {
        if(OPENED.compareAndSet(true, false)) {
            running = false;

            // Stop execution of kafka consumer
            noThrow(consumer::wakeup, "Could not call wakeup on Kafka Consumer.");

            // Interrupt all currently running threads - these will be re-allocated to another Engine.
            noThrow(executor::shutdownNow, "Could shutdown executor pool.");

            LOG.debug("TaskRunner stopped");
        }
        else {
            LOG.error("TaskRunner close() called before open()!");
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        for(ConsumerRecord<String, String> record: records) {
            LOG.debug("Received " + record.key() + "\n Runner currently has tasks: "+ getRunningTasksCount() + " allowed: "+ EXECUTOR_SIZE);

            // Exit loop when TaskRunner capacity full
            if(getRunningTasksCount() >= EXECUTOR_SIZE) {
                seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset());
                break;
            }

            String id = record.key();
            JSONObject configuration = new JSONObject(record.value());

            TaskStatus status = getStatus(id);
            if(status != SCHEDULED) {
                LOG.debug("Cant run this task - " + id + " because\n\t\tstatus: "+ status);
                continue;
            }

            // Submit to executor
            executor.submit(() -> executeTask(id, configuration));

            // Advance offset
            seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset()+1);
        }
    }

    /**
     * Checks to see if task can be marked as running, and does so if possible. Updates TaskState in ZK & Grakn.
     * @param id String id
     * @return Boolean, true if task could be marked as running (and we should run), false otherwise.
     */
    private TaskStatus getStatus(String id) {
        return zkStorage.getState(id).status();
    }

    /**
     * Instantiate a BackgroundTask object and run it, catching any thrown Exceptions.
     * @param id String ID of task as used *both* in ZooKeeper and GraknGraph. This must be the ID generated by Grakn Graph.
     * @param configuration TaskState for task @id.
     */
    private void executeTask(String id, JSONObject configuration) {
        try {
            // Mark as RUNNING and update task & runner states.
            addRunningTask(id);
            updateTaskState(id, RUNNING, this.getClass().getName(), ENGINE_ID, null, null);

            LOG.debug("Executing task " + id);

            // Get full task state.
            TaskState state = graknStorage.getState(id);

            // Instantiate task.
            Class<?> c = Class.forName(state.taskClassName());
            BackgroundTask task = (BackgroundTask) c.newInstance();

            // Run task.
            task.start(saveCheckpoint(id), configuration);

            updateTaskState(id, COMPLETED, this.getClass().getName(), null, null, null);
        }
        catch(Throwable t) {
            LOG.debug("Failed task - "+id+": "+getFullStackTrace(t));
            updateTaskState(id, FAILED, this.getClass().getName(), null, t, null);
        }
        finally {
            removeRunningTask(id);
            LOG.debug("Finished executing task - " + id);
        }
    }

    /**
     * Persists a Background Task's checkpoint to ZK and graph.
     * @param id LID of task
     * @return A Consumer<String> function that can be called by the background task on demand to save its checkpoint.
     */
    private Consumer<String> saveCheckpoint(String id) {
        return checkpoint -> updateTaskState(id, null, null, null, null, checkpoint);
    }

    private void updateTaskState(String id, TaskStatus status, String statusChangeBy, String engineID,
                                 Throwable failure, String checkpoint) {
        LOG.debug("Marking task " + id + " as " + status.name());
        zkStorage.updateState(id, status, engineID, checkpoint);
        graknStorage.updateState(id, status, statusChangeBy, engineID, failure, checkpoint, null);
    }

    private void updateOwnState() {
        JSONArray out = new JSONArray();
        out.put(runningTasks);

        try {
            zkStorage.connection().setData().forPath(RUNNERS_STATE+"/"+ ENGINE_ID, out.toString().getBytes(StandardCharsets.UTF_8));
        }
        catch (Exception e) {
            LOG.error("Could not update TaskRunner taskstorage in ZooKeeper! " + e);
        }
    }

    private void registerAsRunning() throws Exception {
        if(zkStorage.connection().checkExists().forPath(RUNNERS_WATCH + "/" + ENGINE_ID) == null) {
            zkStorage.connection().create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL).forPath(RUNNERS_WATCH + "/" + ENGINE_ID);
        }

        if(zkStorage.connection().checkExists().forPath(RUNNERS_STATE+"/"+ ENGINE_ID) == null) {
            zkStorage.connection().create()
                    .creatingParentContainersIfNeeded()
                    .forPath(RUNNERS_STATE + "/" + ENGINE_ID);
        }

        LOG.debug("Registered TaskRunner");
    }

    private synchronized int getRunningTasksCount() {
        return runningTasks.size();
    }

    private synchronized void addRunningTask(String id) {
        runningTasks.add(id);
        updateOwnState();
    }

    private synchronized void removeRunningTask(String id) {
        runningTasks.remove(id);
        updateOwnState();
    }

    private void seekAndCommit(TopicPartition partition, long offset) {
        consumer.seek(partition, offset);
        consumer.commitSync();
    }

    private void printInitialization() {
        if(!initialised) {
            initialised = true;
            LOG.info("TaskRunner initialised");
        }
    }
}
