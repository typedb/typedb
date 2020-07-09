package grakn.core.server.migrate;

import grakn.core.server.migrate.proto.MigrateProto;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractJob {

    private final String name;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final CountDownLatch finishedLatch = new CountDownLatch(1);
    private volatile Exception error;
    private volatile boolean cancelled;

    protected AbstractJob(String name) {
        this.name = name;
    }

    /**
     * Thread-safe way of retrieving current progress.
     *
     * @return Current progress
     */
    public abstract MigrateProto.Job.Progress getCurrentProgress();

    public abstract MigrateProto.Job.Completion getCompletion();

    protected abstract void executeInternal() throws Exception;

    public String getName() {
        return name;
    }

    /**
     * Called by the main worker thread and blocks until export completion.
     */
    public void execute() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Cannot invoke same job twice");
        }

        try {
            executeInternal();
        } catch (Exception e) {
            error = e;
        } finally {
            finishedLatch.countDown();
        }
    }

    public void cancel() {
        cancelled = true;
    }

    protected boolean isCancelled() {
        return cancelled;
    }

    public boolean awaitCompletion(long timeout, TimeUnit timeUnit) throws Exception {
        try {
            if (finishedLatch.await(timeout, timeUnit)) {
                if (error != null) {
                    throw new ExecutionException(error);
                } else {
                    return true;
                }
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            cancel();
            Thread.currentThread().interrupt();
            throw e;
        }
    }
}
