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

package hypergraph.graph.util;

import hypergraph.common.collection.ByteArray;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class AttributeSync {

    private final ConcurrentMap<ByteArray, CommitFlag> commitFlags;

    public AttributeSync() {
        this.commitFlags = new ConcurrentHashMap<>();
    }

    public CommitFlag get(byte[] attributeIID) {
        return commitFlags.computeIfAbsent(ByteArray.of(attributeIID), iid -> new CommitFlag());
    }

    public static class CommitFlag {

        private final AtomicBoolean commited;

        CommitFlag() {
            commited = new AtomicBoolean(false);
        }

        public boolean getAndSetTrue() {
            return commited.getAndSet(true);
        }
    }
}
