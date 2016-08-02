/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.loader;

import io.mindmaps.postprocessing.BackgroundTasks;
import io.mindmaps.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class QueueManager {
    private final Logger LOG = LoggerFactory.getLogger(QueueManager.class);
    private int maintenanceIteration;
    private AtomicBoolean maintenanceInProcess;
    private AtomicInteger currentJobs;
    private AtomicInteger finishedJobs;

    public int getErrorJobs() {
        return errorJobs.get();
    }

    private AtomicInteger errorJobs;

    public int getFinishedJobs() {
        return finishedJobs.get();
    }

    public int getTotalJobs() {
        return totalJobs.get();
    }

    private AtomicInteger totalJobs;
    private AtomicLong lastJobFinished;
    private ExecutorService executor;
    private Map<UUID, Future> futures;
    private StateVariables loaderState;

    private static QueueManager instance = null;

    public static synchronized QueueManager getInstance() {
        if (instance == null)
            instance = new QueueManager();

        return instance;
    }

    private QueueManager() {
        ConfigProperties prop = ConfigProperties.getInstance();
        maintenanceInProcess = new AtomicBoolean(false);
        currentJobs = new AtomicInteger(0);
        finishedJobs = new AtomicInteger(0);
        errorJobs = new AtomicInteger(0);
        totalJobs = new AtomicInteger(0);
        lastJobFinished = new AtomicLong(0);
        futures = new ConcurrentHashMap<>();
        executor = Executors.newFixedThreadPool(prop.getPropertyAsInt(ConfigProperties.NUM_THREADS_PROPERTY));
        loaderState = new StateVariables();
        maintenanceIteration= prop.getPropertyAsInt(ConfigProperties.MAINTENANCE_ITERATION);
    }

    // add a job to the queue and generate a unique transaction ID
    public UUID addJob(Callable<List<String>> job) {
        if (executor.isShutdown()) {
            return null;
        }

        LOG.info("Adding new job [" + job + "] to QueueManager [" + this + "]");

        UUID uuid = UUID.randomUUID();
        loaderState.addTransaction(uuid);
        futures.put(uuid, executor.submit(() -> handleJob(uuid, job)));
        totalJobs.incrementAndGet();
        return uuid;
    }


    private synchronized void checkStatus() {
        if (maintenanceInProcess.get()) {
            try {
                LOG.info("Waiting for maintenance to complete");
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void unlock() {
        maintenanceInProcess.set(false);
        notifyAll();
        LOG.info("Unlocking QueueManager [" + this + "]");
    }

    public void lock() {
        maintenanceInProcess.set(true);
        LOG.info("Locking QueueManager [" + this + "] for external maintenance");
    }

    // fetch the result of a job by transaction ID if it has finished
    public boolean getResult(UUID uuid) {
        return futures.get(uuid).isDone();
    }

    public void cancelAll() {
        LOG.info("cancelling all transactions");

        for (UUID uuid : futures.keySet()) {
            loaderState.putState(uuid, State.CANCELLED);
        }

        executor.shutdownNow();
    }

    public State getState(UUID uuid) {
        return loaderState.getState(uuid);
    }

    public Map<UUID, State> getStates() {
        return loaderState.getStates();
    }

    public List<String> getStatus(UUID uuid) {
        return loaderState.getStatus(uuid);
    }

    public List<String> getErrors(UUID uuid) {
        return loaderState.getErrors(uuid);
    }

    // allow the thread to be locked externally in testing by exposing futures.get()
    public void getFutureResult(UUID uuid) {
        Future future = futures.get(uuid);
        try {
            future.get();
        } catch (InterruptedException e) {
            loaderState.putState(uuid, State.CANCELLED);
            loaderState.addError(uuid, "Job in queue has thrown interrupted exception.");
            loaderState.addError(uuid, e.getMessage());
            e.printStackTrace();
            printStatus();
        } catch (ExecutionException e) {
            loaderState.putState(uuid, State.ERROR);
            loaderState.addError(uuid, "Job in queue has thrown execution exception.");
            loaderState.addError(uuid, e.getMessage());
            printStatus();
        }
    }

    private void handleJob(UUID uuid, Callable<List<String>> job) {
        checkStatus();
        loaderState.putState(uuid, State.LOADING);
        currentJobs.incrementAndGet();

        List<String> errors = new ArrayList<>();
        // Run job, recording any errors
        try {
            errors.addAll(job.call());
        } catch (Exception e) {
            e.printStackTrace();
            errors.add(e.getClass().getName() + ": " + e.getMessage());
        }
        errors.forEach(s -> loaderState.addError(uuid, s));
        // Change state based on errors
        currentJobs.decrementAndGet();

        synchronized (BackgroundTasks.getInstance()) {
            BackgroundTasks.getInstance().notify();   // why dont we synchronise the notify method?
        }

        lastJobFinished.set(System.currentTimeMillis());

        if (Thread.currentThread().isInterrupted() || loaderState.getState(uuid) == State.CANCELLED) {
            loaderState.putState(uuid, State.CANCELLED);
        } else if (errors.size() != 0) {
            loaderState.putState(uuid, State.ERROR);
            errorJobs.incrementAndGet();
        } else {
            completeJob(uuid);
        }
    }

    private void completeJob(UUID uuid) {
        loaderState.putState(uuid, State.FINISHED);
        long jobs = finishedJobs.incrementAndGet();
        printStatus();
        if (jobs % maintenanceIteration == 0) {
            BackgroundTasks.getInstance().forcePostprocessing();
        }
    }

    private void printStatus() {
        LOG.info("Jobs Finished  : [" + finishedJobs.get() + "/" + totalJobs.get() + "]");
        LOG.info("Jobs Loading  : [" + currentJobs.get() + "/" + totalJobs.get() + "]");
        LOG.info("Jobs Error  : [" + errorJobs.get() + "/" + totalJobs.get() + "]");
    }

    public long getTimeOfLastJob() {
        return lastJobFinished.get();
    }

    public int getNumberOfCurrentJobs() {
        return currentJobs.get();
    }

}
