/*
 * Copyright (C) 2021 Grakn Labs
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
 *
 */

package grakn.core.concurrent.lock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

public class ManagedCountDownLatch {

    private final CountDownLatch countDownLatch;
    private final ThreadLocal<LatchWaiter> latchWaiter;

    public ManagedCountDownLatch(int count) {
        this.countDownLatch = new CountDownLatch(count);
        this.latchWaiter = ThreadLocal.withInitial(LatchWaiter::new);
    }

    public void await() throws InterruptedException {
        ForkJoinPool.managedBlock(latchWaiter.get());
    }

    public long getCount() {
        return countDownLatch.getCount();
    }

    public void countDown() {
        countDownLatch.countDown();
    }

    private class LatchWaiter implements ForkJoinPool.ManagedBlocker {

        @Override
        public boolean block() throws InterruptedException {
            countDownLatch.await();
            return true;
        }

        @Override
        public boolean isReleasable() {
            return countDownLatch.getCount() == 0;
        }
    }
}
