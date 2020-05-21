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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.IID;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoreAttributeSync implements AttributeSync {

    private final Cache<IID.Vertex.Attribute, CoreCommitSync> commitSyncs;

    CoreAttributeSync() { // TODO: extract these values to grakn.properties
        this.commitSyncs = Caffeine.newBuilder()
                .maximumSize(10_000_000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public CoreCommitSync get(IID.Vertex.Attribute attributeIID) {
        return commitSyncs.get(attributeIID, iid -> new CoreCommitSync());
    }

    @Override
    public void remove(IID.Vertex.Attribute attributeIID) {
        commitSyncs.invalidate(attributeIID);
    }

    public static class CoreCommitSync implements AttributeSync.CommitSync {

        private final AtomicBoolean committed;

        CoreCommitSync() {
            committed = new AtomicBoolean(false);
        }

        @Override
        public boolean checkIsSyncedAndSetTrue() {
            return committed.getAndSet(true);
        }
    }
}
