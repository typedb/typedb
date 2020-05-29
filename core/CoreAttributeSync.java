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
 *
 */

package hypergraph.core;

import hypergraph.common.concurrent.ManagedReadWriteLock;
import hypergraph.common.exception.HypergraphException;
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.IID;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class CoreAttributeSync implements AttributeSync, AutoCloseable {

    private final CoreKeyspace keyspace;
    private final LinkedHashMap<IID.Vertex.Attribute, CoreCommitSync> commitSyncs;
    private final ManagedReadWriteLock lock;
    private final int expireDuration;
    private final int evictionCycle;
    private final int memoryReserveMinimum;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledEviction;

    CoreAttributeSync(CoreKeyspace keyspace, int expireDuration, int evictionCycle, int memoryReserveMinimum) {
        this.keyspace = keyspace;
        this.expireDuration = expireDuration;
        this.evictionCycle = evictionCycle;
        this.memoryReserveMinimum = memoryReserveMinimum;
        commitSyncs = new LinkedHashMap<>(1000, 0.75f, true);
        lock = new ManagedReadWriteLock();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduleEviction();
    }

    @Override
    @GuardedBy("lock")
    public CoreCommitSync get(IID.Vertex.Attribute attributeIID) {
        return commitSyncs.computeIfAbsent(attributeIID, iid -> new CoreCommitSync()).accessed(Instant.now());
    }

    @Override
    public void lock() throws InterruptedException {
        lock.lockWrite();
    }

    @Override
    public void unlock() {
        eviction();
        lock.unlockWrite();
    }

    private void scheduleEviction() {
        scheduledEviction = scheduler.scheduleAtFixedRate(() -> {
            try {
                lock();
                eviction();
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                unlock();
            }
        }, evictionCycle, evictionCycle, TimeUnit.SECONDS);
    }

    private void eviction() {
        Instant expiry = Instant.now().minusSeconds(expireDuration);
        Iterator<Map.Entry<IID.Vertex.Attribute, CoreCommitSync>> iterator = commitSyncs.entrySet().iterator();
        Map.Entry<IID.Vertex.Attribute, CoreCommitSync> sync;

        while (iterator.hasNext() &&
                ((sync = iterator.next()).getValue().accessed().compareTo(expiry) < 0 ||
                        Runtime.getRuntime().freeMemory() < memoryReserveMinimum)) {

            if (sync.getValue().snapshot() <= keyspace.oldestWriteSnapshot()) {
                iterator.remove();
                // Important to use .remove() method from LinkedHashMap Set View
                // so that it does not modify the rest of the iteration of the Map
            }
        }
    }

    private void clear() {
        commitSyncs.clear();
    }

    @Override
    public void close() {
        scheduledEviction.cancel(true);
        scheduler.shutdownNow();
        clear();
    }

    public static class CoreCommitSync implements AttributeSync.CommitSync {

        private Status status;
        private long snapshot;
        private Instant accessed;

        CoreCommitSync() {
            status = Status.NONE;
            snapshot = -1;
            accessed = Instant.now();
        }

        @Override
        public Status status() {
            return status;
        }

        @Override
        public void status(Status status) {
            this.status = status;
        }

        @Override
        public long snapshot() {
            return snapshot;
        }

        @Override
        public void snapshot(long snapshot) {
            this.snapshot = snapshot;
        }

        public Instant accessed() {
            return accessed;
        }

        public CoreCommitSync accessed(Instant accessed) {
            this.accessed = accessed;
            return this;
        }
    }
}
