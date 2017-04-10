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

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.postprocessing.ConceptFixer.checkCastings;
import static ai.grakn.engine.postprocessing.ConceptFixer.checkResources;

/**
 * <p>
 *     Post Processing Manager
 * </p>
 *
 * <p>
 *     This organises post processing jobs and divides them out into mutually exclusive jobs.
 *     I.e. jobs which are unlikely to affect each other.
 *     It then calls {@link ConceptFixer} which performs the actual fix.
 *
 * </p>
 *
 * @author fppt
 */
public class PostProcessing {
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.LOG_NAME_POSTPROCESSING_DEFAULT);

    private static PostProcessing instance = null;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private ExecutorService postpool;
    private ExecutorService statDump;
    private Set<Future> futures;

    private PostProcessing() {
        postpool = Executors.newFixedThreadPool(Integer.parseInt(GraknEngineConfig.getInstance().getProperty(GraknEngineConfig.POST_PROCESSING_THREADS)));
        statDump = Executors.newSingleThreadExecutor();
        futures = ConcurrentHashMap.newKeySet();
        isRunning.set(false);
    }

    public static synchronized PostProcessing getInstance() {
        if (instance == null) {
            instance = new PostProcessing();
        }
        return instance;
    }

    public boolean run(String keyspace, String castingIndex, Set<ConceptId> castings, String resourceIndex, Set<ConceptId> resources) {
        if (!isRunning.getAndSet(true)) {
            LOG.info("Starting maintenance.");

            dumpStats(keyspace, castings, resources);

            // Run the post processing
            if(castings.size() > 0) {
                performCastingFix(keyspace, castingIndex, castings);
                waitToContinue();
            }

            if(resources.size() > 0) {
                performResourceFix(keyspace, resourceIndex, resources);
                waitToContinue();
            }

            futures = ConcurrentHashMap.newKeySet();
            boolean notCancelled = isRunning.getAndSet(false);

            LOG.info("Maintenance completed.");

            return notCancelled;
        } else {
            return true;
        }
    }

    public boolean stop() {
        boolean running = isRunning.getAndSet(false);
        
        if(running) {
            LOG.warn("Shutting down running tasks");
            System.out.println("Shutting down running tasks");
            futures.forEach(f -> f.cancel(true));
            postpool.shutdownNow();
            statDump.shutdownNow();
        }

        return running;
    }

    private void performCastingFix(String keyspace, String index, Set<ConceptId> castingsToPP) {
        try {
            futures.add(postpool.submit(() -> checkCastings(keyspace, index, castingsToPP)));
        } catch (RuntimeException e) {
            LOG.error("Error while trying to perform post processing on graph [" + keyspace + "]",e);
        }
    }

    private void performResourceFix(String keyspace, String index, Set<ConceptId> resourcesToPP){
        try {
            futures.add(postpool.submit(() -> checkResources(keyspace, index, resourcesToPP)));
        } catch (RuntimeException e) {
            LOG.error("Error while trying to perform post processing on graph [" + keyspace + "]",e);
        }
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

    private void dumpStats(String keyspace, Set<ConceptId> castings, Set<ConceptId> resources) {
        LOG.info("--------------------Current Status of Post Processing--------------------");
        LOG.info("Keyspace      : " + keyspace);
        LOG.info("Castings      : " + castings.size());
        LOG.info("Resources     : " + resources.size());
        LOG.info("-------------------------------------------------------------------------");
    }
}
