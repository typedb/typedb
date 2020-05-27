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
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.IID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CoreAttributeSync implements AttributeSync {

    private final ConcurrentMap<IID.Vertex.Attribute, CoreCommitSync> commitSyncs;
    private final ManagedReadWriteLock lock;

    CoreAttributeSync() { // TODO: extract these values to grakn.properties
        commitSyncs = new ConcurrentHashMap<>();
        lock = new ManagedReadWriteLock();
    }

    @Override
    public CoreCommitSync get(IID.Vertex.Attribute attributeIID) {
        return commitSyncs.computeIfAbsent(attributeIID, iid -> new CoreCommitSync());
    }

    @Override
    public void remove(IID.Vertex.Attribute attributeIID) {
        commitSyncs.remove(attributeIID);
    }

    @Override
    public void lock() throws InterruptedException {
        lock.lockWrite();
    }

    @Override
    public void unlock() {
        lock.unlockWrite();
    }

    public static class CoreCommitSync implements AttributeSync.CommitSync {

        private Status status;
        private long snapshot;

        CoreCommitSync() {
            status = Status.NONE;
            snapshot = -1;
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
    }
}
