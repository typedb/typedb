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

package ai.grakn.engine.postprocessing;

import ai.grakn.factory.GraphFactory;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostProcessing {
    private static final String CASTING_STAGE = "Scanning for duplicate castings . . .";
    private static final String RESOURCE_STAGE = "Scanning for duplicate resources . . .";

    private static PostProcessing instance = null;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Logger LOG = LoggerFactory.getLogger(PostProcessing.class);

    private ExecutorService postpool;
    private ExecutorService statDump;
    private Set<Future> futures;
    private String currentStage;
    private Cache cache;

    private PostProcessing() {
        postpool = Executors.newFixedThreadPool(ConfigProperties.getInstance().getAvailableThreads());
        statDump = Executors.newSingleThreadExecutor();
        cache = Cache.getInstance();
        futures = ConcurrentHashMap.newKeySet();
        isRunning.set(false);
    }

    public static synchronized PostProcessing getInstance() {
        if (instance == null)
            instance = new PostProcessing();
        return instance;
    }

    public void run() {
        if (!isRunning.get()) {
            LOG.info("Starting maintenance.");
            isRunning.set(true);

            statDump.submit(this::dumpStats);
            performTasks();

            futures = ConcurrentHashMap.newKeySet();
            isRunning.set(false);
            LOG.info("Maintenance completed.");
        }
    }

    public void stop() {
        if(isRunning.get()) {
            LOG.warn("Shutting down running tasks");
            futures.forEach(f -> f.cancel(true));
            postpool.shutdownNow();
            statDump.shutdownNow();
        }

        isRunning.set(false);
    }

    public void reset() {
        isRunning.set(false);
        futures.clear();
        postpool = Executors.newFixedThreadPool(ConfigProperties.getInstance().getAvailableThreads());
        statDump = Executors.newSingleThreadExecutor();
    }

    private void performTasks() {
        currentStage = CASTING_STAGE;
        LOG.info(currentStage);
        performCastingFix();
        waitToContinue();

        currentStage = RESOURCE_STAGE;
        LOG.info(currentStage);
        performResourceFix();
        waitToContinue();
    }

    private void performCastingFix() {
        cache.getKeyspaces().parallelStream().forEach(keyspace -> {
            try {
                Set<String> castingIds = new HashSet<>();
                castingIds.addAll(cache.getCastingJobs(keyspace));

                for (String castingId : castingIds) {
                    futures.add(postpool.submit(() ->
                            ConceptFixer.checkCasting(cache, GraphFactory.getInstance().getGraphBatchLoading(keyspace), castingId)));
                }
            } catch (RuntimeException e) {
                LOG.error("Error while trying to perform post processing on graph [" + keyspace + "]",e);
            }

        });
    }

    private void performResourceFix(){
        cache.getKeyspaces().parallelStream().forEach(keyspace -> {
            try {
                futures.add(postpool.submit(() ->
                        ConceptFixer.checkResources(cache, GraphFactory.getInstance().getGraphBatchLoading(keyspace), cache.getResourceJobs(keyspace))));
            } catch (RuntimeException e) {
                LOG.error("Error while trying to perform post processing on graph [" + keyspace + "]",e);
            }
        });
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
            dumpStatsType("Casting");
            dumpStatsType("Resources");
            LOG.info("Save in Progress: " + cache.isSaveInProgress());
            LOG.info("Current Stage: " + currentStage);
            LOG.info("-------------------------------------------------------------------------");

            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                LOG.error("Exception",e);
            }
        }
    }

    private void dumpStatsType(String typeName) {
        long total = 0L;
        LOG.info(typeName + " Jobs:");

        for (String keyspace : cache.getKeyspaces()) {
            long numJobs = 0L;
            if(typeName.equals("Casting")){
                numJobs = cache.getCastingJobs(keyspace).size();
            } else if(typeName.equals("Resources")){
                numJobs = cache.getCastingJobs(keyspace).size();
            }
            LOG.info("        Post processing step [" + typeName + " for Graph [" + keyspace + "] has jobs : " + numJobs);
            total += numJobs;
        }

        LOG.info("    Total " + typeName + " Jobs: " + total);
    }
}
