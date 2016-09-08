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

package io.mindmaps.engine.postprocessing;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.factory.MindmapsClient;
import io.mindmaps.engine.loader.RESTLoader;
import org.apache.tinkerpop.shaded.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BackgroundTasks {
    private static long timeLapse;
    private static final String CASTING_STAGE = "Scanning for duplicate castings . . .";
    private static final String RELATION_STAGE = "Scanning for duplicate relations . . .";

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Logger LOG = LoggerFactory.getLogger(BackgroundTasks.class);
    private ExecutorService postpool;
    private ExecutorService statDump;
    private Set<Future> futures;
    private String currentStage;

    private static BackgroundTasks instance = null;

    private Cache cache;


    public static synchronized BackgroundTasks getInstance() {
        if (instance == null)
            instance = new BackgroundTasks();
        return instance;
    }

    public boolean isPostProcessingRunning() {
        return isRunning.get();
    }

    private BackgroundTasks() {
        timeLapse = ConfigProperties.getInstance().getPropertyAsLong(ConfigProperties.TIME_LAPSE);
        postpool = Executors.newFixedThreadPool(ConfigProperties.getInstance().getAvailableThreads());
        statDump = Executors.newSingleThreadExecutor();
        cache = Cache.getInstance();
        futures = ConcurrentHashMap.newKeySet();
        isRunning.set(false);
    }

    public void performPostprocessing() {
        futures = ConcurrentHashMap.newKeySet();

        if (maintenanceAllowed()) {
            postprocessing();
        }
    }

    public void forcePostprocessing() {
        postprocessing();
    }

    private void postprocessing() {
        if (!isRunning.get()) {
            LOG.info("Starting maintenance.");
            isRunning.set(true);
            statDump.submit(this::dumpStats);
            performTasks();
            isRunning.set(false);
            LOG.info("Maintenance completed.");
        }
    }

    private void performTasks() {
        currentStage = CASTING_STAGE;
        LOG.info(currentStage);
        performCastingFix();

        waitToContinue();
    }

    private void performCastingFix() {
        cache.getCastingJobs().entrySet().parallelStream().forEach(entry -> {

            MindmapsGraph graph;
            try {
                graph = GraphFactory.getInstance().getGraph(entry.getKey());
                for (String castingId : entry.getValue()) {
                    futures.add(postpool.submit(() -> ConceptFixer.checkCasting(graph, castingId)));
                }
            } catch (RuntimeException e) {
                LOG.error("Error while trying to perform post processing on graph [" + entry.getKey() + "]");
                e.printStackTrace();
            }

        });
    }

    private boolean maintenanceAllowed() {
        long lastJob = RESTLoader.getInstance().getLastJobFinished();
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastJob) >= timeLapse && RESTLoader.getInstance().getLoadingJobs() == 0;
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

    private void dumpStatsType(String typeName, Map<String, Set<String>> jobs) {
        long total = 0L;
        LOG.info(typeName + " Jobs:");
        for (Map.Entry<String, Set<String>> entry : jobs.entrySet()) {
            Log.info("        Post processing step [" + typeName + " for Graph [" + entry.getKey() + "] has jobs : " + entry.getValue().size());
            total += entry.getValue().size();
        }
        LOG.info("    Total " + typeName + " Jobs: " + total);
    }
}
