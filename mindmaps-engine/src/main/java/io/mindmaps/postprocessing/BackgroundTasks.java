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

package io.mindmaps.postprocessing;

import io.mindmaps.factory.GraphFactory;
import io.mindmaps.loader.QueueManager;
import io.mindmaps.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BackgroundTasks {
    private static final long TIME_LAPSE = 60000;

    private final AtomicBoolean canRun = new AtomicBoolean(true);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Logger LOG = LoggerFactory.getLogger(BackgroundTasks.class);
    private ExecutorService postpool;
    private ExecutorService statDump;
    private Set<Future> futures;
    private ConceptFixer conceptFixer;
    private String currentStage;

    private static BackgroundTasks instance = null;

    private Cache cache;


    public static synchronized BackgroundTasks getInstance() {
        if (instance == null)
            instance = new BackgroundTasks();
        return instance;
    }

    private BackgroundTasks() {
        conceptFixer = new ConceptFixer(cache, GraphFactory.getInstance());
        postpool = Executors.newFixedThreadPool(ConfigProperties.getInstance().getPropertyAsInt(ConfigProperties.NUM_THREADS_PROPERTY));
        statDump = Executors.newSingleThreadExecutor();
        cache = Cache.getInstance();
    }

    public void performPostprocessing() {
        futures = ConcurrentHashMap.newKeySet();

        if (maintenanceAllowed()) {
            isRunning.set(true);
            statDump.submit(this::dumpStats);
            postprocessing();
            isRunning.set(false);
        }
    }

    public void forcePostprocessing() {
        postprocessing();
    }

    private void postprocessing() {
        LOG.info("Starting maintenance and locking QueueManager");
        lockQueueManager();
        performTasks();
        QueueManager.getInstance().unlock();
        LOG.info("Maintenance completed and unlocking QueueManager");
    }

    private void lockQueueManager() {
        synchronized (this) {
            QueueManager.getInstance().lock();
            while (QueueManager.getInstance().getNumberOfCurrentJobs() != 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    LOG.error("Error while waiting for lock", e);
                }
            }
            LOG.info("QueueManager safely locked. Proceeding with maintenance");
        }
    }

    private void performTasks() {
        LOG.info("Checking castings . . .");
        currentStage = "Starting. . . ";
        performCastingFix();

        LOG.info("Scanning for duplicate assertions . . .");
        performDuplicateAssertionFix();

        LOG.info("Inserting REAL shortcuts . . .");
        performShortcutFix();
        currentStage = "Done";
    }

    private void performDuplicateAssertionFix() {
        currentStage = "Scanning for duplicate assertions";
        Set<Future<String>> duplicateAssertionsJob = ConcurrentHashMap.newKeySet();
        Set<String> uniqueAssertionHashCodes = new HashSet<>();

        cache.getRelationJobs().entrySet().parallelStream().forEach(inner -> {
            inner.getValue().parallelStream().forEach(assertionId -> {
                duplicateAssertionsJob.add(postpool.submit(() -> conceptFixer.createAssertionHashCode(assertionId)));
            });
        });

        LOG.info("Assertions scanned. Checking for duplicates . . . ");

        for (Future future : duplicateAssertionsJob) {
            try {
                String result = (String) future.get();

                String[] vals = result.split("_");
                if (vals.length != 2)
                    continue;
                String assertionId = vals[0];
                String assertionHashCode = vals[1];

                if (uniqueAssertionHashCodes.contains(assertionHashCode)) {
                    LOG.info("Deleting duplicate assertion [" + assertionId + "] which has hash-code [" + assertionHashCode + "]");
                    futures.add(postpool.submit(() -> conceptFixer.deleteDuplicateAssertion(Long.parseLong(assertionId))));
                } else {
                    uniqueAssertionHashCodes.add(assertionHashCode);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error while waiting for job to complete on fixing duplicate assertions.", e);
            }
        }

        LOG.info("Waiting for duplicate assertion fix to complete");
        waitToContinue();
    }

    private void performCastingFix() {
        //TODO: Fix duplicate castings.
    }

    private void performShortcutFix() {
        //TODO: Fix duplicate shortcuts which may exist.
    }

    public void cancelPostProcessing() {
        LOG.info("Cancelling post processing . . .");
        canRun.set(false);
        postpool.shutdownNow();
    }

    private boolean maintenanceAllowed() {
        if (!canRun.get())
            return false;

        long lastJob = QueueManager.getInstance().getTimeOfLastJob();
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastJob) >= TIME_LAPSE && QueueManager.getInstance().getNumberOfCurrentJobs() == 0;
    }

    private void waitToContinue() {
        for (Future future : futures) {
            try {
                future.get(4, TimeUnit.HOURS);
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error while waiting for future: ", e);
            } catch (TimeoutException e) {
                LOG.warn("Timeout exception waiting for future to complete", e);
            }
        }
        futures.clear();
    }

    private void dumpStats() {
        while (isRunning.get()) {
            LOG.info("--------------------Current Status of Post Processing--------------------");
            dumpStatsType("Casting", cache.getCastingJobs());
            dumpStatsType("Assertion", cache.getRelationJobs());
            LOG.info("Save in Progress: " + cache.isSaveInProgress());
            LOG.info("Current Stage: " + currentStage);
            LOG.info("-------------------------------------------------------------------------");

            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void dumpStatsType(String typeName, Map map) {
        int total = 0;
        LOG.info(typeName + " Jobs:");
        Set<String> keys = map.keySet();
        for (String type : keys) {
            Object object = map.get(type);
            int numJobs = 0;
            if (object instanceof Map)
                numJobs = ((Map) object).size();
            else if (object instanceof Set)
                numJobs = ((Set) object).size();

            total = total + numJobs;
            if (numJobs != 0)
                LOG.info("        " + typeName + " Type [" + type + "] has jobs : " + numJobs);
        }
        LOG.info("    Total " + typeName + " Jobs: " + total);
    }
}
