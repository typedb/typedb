/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graph.util.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public abstract class BackgroundThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(BackgroundThread.class);

    private volatile boolean interruptible = true;
    private volatile boolean softInterrupted = false;

    /**
     * NEVER set daemon=true and override the cleanup() method. If this is a daemon thread there is no guarantee that
     * cleanup is called.
     */
    public BackgroundThread(String name, boolean daemon) {
        this.setName(name + ":" + getId());
        this.setDaemon(daemon);
    }

    @Override
    public void run() {

        /* We use interrupted() instead of isInterrupted() to guarantee that the
         * interrupt flag is cleared when we exit this loop. cleanup() can then
         * run blocking operations without failing due to interruption.
         */
        while (!interrupted() && !softInterrupted) {

            try {
                waitCondition();
            } catch (InterruptedException e) {
                LOG.debug("Interrupted in background thread wait condition", e);
                break;
            }

            /* This check could be removed without affecting correctness. At
             * worst, removing it should just reduce shutdown responsiveness in
             * a couple of corner cases:
             *
             * 1. Rare interruptions are those that occur while this thread is
             * in the RUNNABLE state
             *
             * 2. Odd waitCondition() implementations that swallow an
             * InterruptedException and set the interrupt status instead of just
             * propagating the InterruptedException to us
             */
            if (interrupted()) {
                break;
            }

            interruptible = false;
            try {
                action();
            } catch (Throwable e) {
                LOG.error("Exception while executing action on background thread", e);
            } finally {
                /*
                 * This doesn't really need to be in a finally block as long as
                 * we catch Throwable, but it's here as future-proofing in case
                 * the catch-clause type is narrowed in future revisions.
                 */
                interruptible = true;
            }
        }

        try {
            cleanup();
        } catch (Throwable e) {
            LOG.error("Exception while executing cleanup on background thread", e);
        }

    }

    /**
     * The wait condition for the background thread. This determines what this background thread is waiting for in
     * its execution. This might be elapsing time or availability of resources.
     * <p>
     * Since there is a wait involved, this method should throw an InterruptedException
     */
    protected abstract void waitCondition() throws InterruptedException;

    /**
     * The action taken by this background thread when the wait condition is met.
     * This action should execute swiftly to ensure that this thread can be closed in a reasonable amount of time.
     * <p>
     * This action will not be interrupted by #close(Duration).
     */
    protected abstract void action();

    /**
     * Any clean up that needs to be done before this thread is closed down.
     */
    protected void cleanup() {
        //Do nothing by default
    }

    public void close(Duration duration) {

        if (!isAlive()) {
            LOG.warn("Already closed: {}", this);
            return;
        }

        long maxWaitMs = duration.toMillis();

        softInterrupted = true;

        if (interruptible) {
            interrupt();
        }

        try {
            join(maxWaitMs);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for thread to join", e);
        }
        if (isAlive()) {
            LOG.error("Thread {} did not terminate in time [{}]. This could mean that important clean up functions could not be called.", getName(), maxWaitMs);
        }
    }

}
