/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

package ai.grakn.engine.backgroundtasks;

import ai.grakn.engine.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class InMemoryTaskManager extends AbstractTaskManager {
    private static String STATUS_MESSAGE_SCHEDULED = "Task scheduled.";
    private final Logger LOG = LoggerFactory.getLogger(InMemoryTaskManager.class);
    private static InMemoryTaskManager instance = null;

    private Map<UUID, TaskState> taskStateStorage;
    private Map<UUID, ScheduledFuture<BackgroundTask>> taskStorage;

    private ScheduledExecutorService executorService;

    private InMemoryTaskManager() {
        taskStateStorage = new ConcurrentHashMap<>();
        taskStorage = new ConcurrentHashMap<>();

        ConfigProperties properties = ConfigProperties.getInstance();
        // One thread is reserved for the supervisor
        executorService = Executors.newScheduledThreadPool(properties.getAvailableThreads()-1);
        //FIXME: get from config
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::supervisor, 2000, 1000, TimeUnit.MILLISECONDS);
    }

    public static synchronized InMemoryTaskManager getInstance() {
        if (instance == null)
            instance = new InMemoryTaskManager();
        return instance;
    }

    public TaskManager stopTask(UUID uuid) {
        return stopTask(uuid, null, null);
    }

    public TaskManager pauseTask(UUID uuid) {
        return pauseTask(uuid, null, null);
    }

    public TaskManager resumeTask(UUID uuid) {
        return resumeTask(uuid, null, null);
    }

    public TaskManager restartTask(UUID uuid) {
        return restartTask(uuid, null, null);
    }

    public TaskState getTaskState(UUID uuid) {
        return taskStateStorage.get(uuid);
    }

    public Set<UUID> getAllTasks() {
        return taskStateStorage.keySet();
    }

    public Set<UUID> getTasks(TaskStatus taskStatus) {
        return taskStateStorage.entrySet().stream()
                .filter(x -> x.getValue().getStatus() == taskStatus)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }




    protected UUID saveNewState(TaskState state) {
        state.setStatus(TaskStatus.SCHEDULED)
             .setStatusChangeMessage(STATUS_MESSAGE_SCHEDULED)
             .setStatusChangedBy(InMemoryTaskManager.class.getName())
             .setQueuedTime(new Date());

       UUID uuid = UUID.randomUUID();
       taskStateStorage.put(uuid, state);
       return uuid;
    }

    protected UUID updateTaskState(UUID uuid, TaskState state) {
        taskStateStorage.put(uuid, state);
        return uuid;
    }

    protected BackgroundTask instantiateTask(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> c = Class.forName(className);
        return (BackgroundTask) c.newInstance();
    }

    protected void executeSingle(UUID uuid, BackgroundTask task, long delay) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.schedule(
                runTask(uuid, task::start), delay, TimeUnit.MILLISECONDS);
        taskStorage.put(uuid, f);
    }

    protected void executeRecurring(UUID uuid, BackgroundTask task, long delay, long interval) {
        ScheduledFuture<BackgroundTask> f = (ScheduledFuture<BackgroundTask>) executorService.scheduleAtFixedRate(
                runTask(uuid, task::start), delay, interval, TimeUnit.MILLISECONDS);
        taskStorage.put(uuid, f);
    }

    private Runnable runTask(UUID uuid, Runnable fn) {
        return () -> {
            TaskState state = taskStateStorage.get(uuid);
            state.setStatus(TaskStatus.RUNNING);
            taskStateStorage.put(uuid, state);
            fn.run();
        };
    }

    protected ScheduledFuture<BackgroundTask> getTaskExecutionStatus(UUID uuid) {
        return taskStorage.get(uuid);
    }






    private void supervisor() {
        taskStorage.entrySet().forEach(x -> {
            // Validity check
            if(!taskStateStorage.containsKey(x.getKey())) {
                LOG.error("INTERNAL STATE DESYNCHRONISED: taskStorage("+x.getKey().toString()+") is not present in taskStateStorage! CANCELLING TASK.");
                x.getValue().cancel(true);
                taskStorage.remove(x.getKey());
            }

            // Update state of current tasks
            else {
                ScheduledFuture<BackgroundTask> f = x.getValue();
                TaskState state = taskStateStorage.get(x.getKey());

                if(f.isDone()) {
                    state.setStatus(TaskStatus.COMPLETED);
                }

                else if(f.isCancelled()) {
                    if(state.getStatus() != TaskStatus.STOPPED)
                        state.setStatus(TaskStatus.DEAD)
                        .setStatusChangedBy(this.getClass().getName()+" supervisor");
                }

                taskStateStorage.put(x.getKey(), state);
            }
        });
    }
}
