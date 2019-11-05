/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.graphdb.log;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.log.TransactionId;

import java.time.Instant;
import java.util.Objects;

public class StandardTransactionId implements TransactionId {

    private final String instanceId;
    private final long transactionId;
    private final Instant transactionTime;

    public StandardTransactionId(String instanceId, long transactionId, Instant transactionTime) {
        Preconditions.checkArgument(instanceId != null && transactionId >= 0 && transactionTime != null);
        this.instanceId = instanceId;
        this.transactionId = transactionId;
        this.transactionTime = transactionTime;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public Instant getTransactionTime() {
        return transactionTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, transactionId, transactionTime);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (!getClass().isInstance(oth)) return false;
        StandardTransactionId id = (StandardTransactionId) oth;
        return instanceId.equals(id.instanceId) && transactionId == id.transactionId
                && transactionTime.equals(id.transactionTime);
    }

    @Override
    public String toString() {
        return transactionId + "@" + instanceId + "::" + transactionTime;
    }

}
