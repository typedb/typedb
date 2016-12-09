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

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedState;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.EngineID;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.CreateMode;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import static ai.grakn.engine.backgroundtasks.TaskStatus.*;
import static ai.grakn.engine.backgroundtasks.config.ConfigHelper.kafkaConsumer;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.TASK_RUNNER_GROUP;
import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.WORK_QUEUE_TOPIC;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASK_LOCK_SUFFIX;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.TASKS_PATH_PREFIX;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_STATE;
import static ai.grakn.engine.backgroundtasks.config.ZookeeperPaths.RUNNERS_WATCH;

import static ai.grakn.engine.util.ConfigProperties.TASKRUNNER_POLLING_FREQ;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

public class TaskRunner implements Runnable, AutoCloseable {
    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private final static ConfigProperties properties = ConfigProperties.getInstance();

    private ExecutorService executor;
    private final Integer allowableRunningTasks;
    private final Set<String> runningTasks = new HashSet<>();
    private final String engineID = EngineID.getInstance().id();
    private final CountDownLatch countDownLatch;

    private StateStorage graknStorage;
    private SynchronizedStateStorage zkStorage;
    private KafkaConsumer<String, String> consumer;

    TaskRunner(CountDownLatch countDownLatch) {
        allowableRunningTasks = properties.getAvailableThreads();
        graknStorage = new GraknStateStorage();
        this.countDownLatch = countDownLatch;
    }

    /**
     * Start the main loop, this will block until a call to stop().
     */
    public void run()  {
        try {
            zkStorage = SynchronizedStateStorage.getInstance();
            consumer = kafkaConsumer(TASK_RUNNER_GROUP);
            consumer.subscribe(singletonList(WORK_QUEUE_TOPIC), new RebalanceListener(consumer));

            // Create initial entries in ZK for TaskFailover to watch.
            registerAsRunning();
            updateOwnState();
            executor = Executors.newFixedThreadPool(properties.getAvailableThreads());

            countDownLatch.countDown();

            while (true) {
                // Poll for new tasks only when we know we have space to accept them.
                if (getRunningTasksCount() < allowableRunningTasks) {
                    ConsumerRecords<String, String> records = consumer.poll(properties.getPropertyAsInt(TASKRUNNER_POLLING_FREQ));
                    processRecords(records);
                } else {
                    Thread.sleep(500);
                }
            }

        }
        catch (WakeupException|InterruptedException e) {
            LOG.debug(getFullStackTrace(e));
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            LOG.error("Could not start TaskRunner - "+getFullStackTrace(e));
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }

    /**
     * Stop the main loop, causing run() to exit.
     */
    public void close() {
        consumer.wakeup();
        executor.shutdown();
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        for(ConsumerRecord<String, String> record: records) {
            LOG.debug("Got a record\n\t\tkey: "+record.key()+"\n\t\toffset "+record.offset()+"\n\t\tvalue "+record.value());
            LOG.debug("Runner currently has tasks: "+getRunningTasksCount()+" allowed: "+allowableRunningTasks);
            if(getRunningTasksCount() >= allowableRunningTasks) {
                seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset());
                break;
            }

            String id = record.key();
            InterProcessMutex mutex = acquireMutex(id);
            if(mutex == null) {
                seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset());
                break;
            }

            // Check if its marked as SCHEDULED.
            TaskStatus status = getStatus(id);
            if(status == null) {
                seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset());
                releaseMutex(mutex, id);
                break;
            }
            else if(status != SCHEDULED) {
                LOG.debug("Cant schedule this task - "+id+" because\n\t\tstatus: "+status);
                releaseMutex(mutex, id);
                continue;
            }

            // Mark as RUNNING and update task & runner states.
            addRunningTask(id);
            updateTaskState(id, RUNNING, this.getClass().getName(), engineID, null, null);

            releaseMutex(mutex, id);

            // Submit to executor
            try {
                JSONObject configuration = new JSONObject(record.value());
                executor.submit(() -> executeTask(id, configuration));
            }
            catch (RejectedExecutionException | NullPointerException e) {
                LOG.error(getFullStackTrace(e));
                removeRunningTask(id);
            }

            // Advance offset
            LOG.debug("Runner next read from " + record.key() + " OFFSET " + (record.offset()+1) + " topic " + record.topic());
            seekAndCommit(new TopicPartition(record.topic(), record.partition()), record.offset()+1);
        }
    }

    /**
     * Checks to see if task can be marked as running, and does so if possible. Updates TaskState in ZK & Grakn.
     * @param id String id
     * @return Boolean, true if task could be marked as running (and we should run), false otherwise.
     */
    private TaskStatus getStatus(String id) {
        SynchronizedState state = zkStorage.getState(id);
        if (state == null) {
            LOG.error("Cant run task - " + id + " - because zkStorage returned null");
            return null;
        }

        return state.status();
    }

    /**
     * Instantiate a BackgroundTask object and run it, catching any thrown Exceptions.
     * @param id String ID of task as used *both* in ZooKeeper and GraknGraph. This must be the ID generated by Grakn Graph.
     * @param configuration TaskState for task @id.
     */
    private void executeTask(String id, JSONObject configuration) {
        try {
            LOG.debug("Executing task " + id);

            // Get full task state.
            TaskState state = graknStorage.getState(id);

            LOG.debug("Got state of " + id + " from storage");

            // Instantiate task.
            Class<?> c = Class.forName(state.taskClassName());
            BackgroundTask task = (BackgroundTask) c.newInstance();

            // Run task.
            task.start(saveCheckpoint(id), configuration);

            LOG.debug("Task - "+id+" completed successfully, updating state in graph");
            updateTaskState(id, COMPLETED, this.getClass().getName(), null, null, null);
        }
        catch(Throwable t) {
            LOG.debug("Failed task - "+id+": "+getFullStackTrace(t));
            updateTaskState(id, FAILED, this.getClass().getName(), null, t, null);
            LOG.debug("Updated state " + id);
        }
        finally {
            removeRunningTask(id);
            LOG.debug("Finished executing task - " + id);
        }
    }

    /**
     * Returns a new InterProcessMutex object, creating ZNodes if needed
     * @param id String id of task that this lock should be associated to.
     * @return InterProcessMutex object
     */
    private InterProcessMutex acquireMutex(String id) {
        InterProcessMutex mutex = null;
        try {
            if(zkStorage.connection().checkExists().forPath(TASKS_PATH_PREFIX+"/"+id+TASK_LOCK_SUFFIX) == null)
                zkStorage.connection().create().creatingParentContainersIfNeeded().forPath(TASKS_PATH_PREFIX+"/"+id+TASK_LOCK_SUFFIX);

            mutex = new InterProcessMutex(zkStorage.connection(), TASKS_PATH_PREFIX+"/"+id+TASK_LOCK_SUFFIX);

            if (!mutex.acquire(5000, MILLISECONDS)) {
                LOG.debug("Could not acquire mutex");
                mutex = null;
            }
        }
        catch (Exception e) {
            LOG.debug("Exception whilst trying to get mutex for task - " + id + " - " + getFullStackTrace(e));
        }

        LOG.debug("<<<<<<<<<<<< Got mutex for - "+id);
        return mutex;
    }

    private void releaseMutex(InterProcessMutex mutex, String id) {
        try {
            mutex.release();
            LOG.debug(">>>>>>>>>>>> released mutex for - "+id);
        }
        catch (Exception e) {
            LOG.error("********************************\nCOULD NOT RELEASE MUTEX FOR TASK - " + id + "\n" + getFullStackTrace(e) + "********************************\n");
        }
    }

    /**
     * Persists a Background Task's checkpoint to ZK and graph.
     * @param id ID of task
     * @return A Consumer<String> function that can be called by the background task on demand to save its checkpoint.
     */
    private Consumer<String> saveCheckpoint(String id) {
        return checkpoint -> {
            LOG.debug("Writing checkpoint");
            updateTaskState(id, null, null, null, null, checkpoint);
        };
    }

    private void updateTaskState(String id, TaskStatus status, String statusChangeBy, String engineID,
                                 Throwable failure, String checkpoint) {
        LOG.debug("Updating state of task " + id);
        zkStorage.updateState(id, status, engineID, checkpoint);
        try {
            graknStorage.updateState(id, status, statusChangeBy, engineID, failure, checkpoint, null);
        } catch (Exception ignored) {}
    }

    private void updateOwnState() {
        JSONArray out = new JSONArray();
        out.put(runningTasks);

        try {
            zkStorage.connection().setData().forPath(RUNNERS_STATE+"/"+ engineID, out.toString().getBytes());
        }
        catch (Exception e) {
            LOG.error("Could not update TaskRunner taskstorage in ZooKeeper! " + e);
        }
    }

    private void registerAsRunning() throws Exception {
        if(zkStorage.connection().checkExists().forPath(RUNNERS_WATCH + "/" + engineID) == null) {
            zkStorage.connection().create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL).forPath(RUNNERS_WATCH + "/" + engineID);
        }

        if(zkStorage.connection().checkExists().forPath(RUNNERS_STATE+"/"+ engineID) == null) {
            zkStorage.connection().create()
                    .creatingParentContainersIfNeeded()
                    .forPath(RUNNERS_STATE + "/" + engineID);
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
}
