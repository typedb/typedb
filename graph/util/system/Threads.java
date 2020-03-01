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

/**
 * Utility class for dealing with Thread
 */
public class Threads {

    public static boolean oneAlive(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) return true;
        }
        return false;
    }

    public static void terminate(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) thread.interrupt();
        }
    }

    public static final int DEFAULT_SLEEP_INTERVAL_MS = 100;

    public static boolean waitForCompletion(Thread[] threads) {
        return waitForCompletion(threads,Integer.MAX_VALUE);
    }

    public static boolean waitForCompletion(Thread[] threads, int maxWaitMillis) {
        return waitForCompletion(threads,maxWaitMillis,DEFAULT_SLEEP_INTERVAL_MS);
    }

    public static boolean waitForCompletion(Thread[] threads, int maxWaitMillis, int sleepPeriodMillis) {
        long endTime = System.currentTimeMillis()+maxWaitMillis;
        while (oneAlive(threads)) {
            long currentTime = System.currentTimeMillis();
            if (currentTime>=endTime) return false;
            try {
                Thread.sleep(Math.min(sleepPeriodMillis,endTime-currentTime));
            } catch (InterruptedException e) {
                System.err.println("Interrupted while waiting for completion of threads!");
            }
        }
        return true;
    }

}
