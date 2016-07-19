package io.mindmaps.core;

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.loader.QueueManager;
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

    private int NUM_THREADS = 20; // read from config file

    public static synchronized BackgroundTasks getInstance() {
        if (instance == null)
            instance = new BackgroundTasks();
        return instance;
    }

    private BackgroundTasks() {
        conceptFixer = new ConceptFixer(cache, GraphFactory.getInstance());
        postpool = Executors.newFixedThreadPool(NUM_THREADS);
        statDump = Executors.newSingleThreadExecutor();
        cache = Cache.getInstance();
    }

    //@Scheduled(fixedDelay = 300000)
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

        cache.getAssertionJobs().entrySet().parallelStream().forEach(inner -> {
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
        currentStage = "Merging Castings";
        cache.getCastingJobs().entrySet().parallelStream().forEach(inner -> {
            LOG.info("Merging type [" + inner.getKey() + "] which has [" + inner.getValue().size() + "] potential casting sets . . . ");
            inner.getValue().keySet().parallelStream().forEach(key -> futures.add(postpool.submit(() -> conceptFixer.fixElements(DataType.BaseType.CASTING, inner.getKey(), key))));
        });
        LOG.info("Waiting for casting fix to complete");
        waitToContinue();
    }

    private void performShortcutFix() {
        currentStage = "Inserting shortcut edges";
        cache.getAssertionJobs().entrySet().parallelStream().forEach(inner -> {
            LOG.info("Adding to type [" + inner.getKey() + "] shortcuts [" + inner.getValue().size() + "]");
            inner.getValue().parallelStream().forEach(conceptId -> futures.add(postpool.submit(() -> conceptFixer.fixElements(DataType.BaseType.RELATION, inner.getKey(), String.valueOf(conceptId)))));
        });
        LOG.info("Waiting for assertion shortcut fix to complete");
        waitToContinue();
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
            dumpStatsType("Assertion", cache.getAssertionJobs());
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
