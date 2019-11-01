// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        org.janusgraph.graphdb.log.StandardTransactionId id = (org.janusgraph.graphdb.log.StandardTransactionId) oth;
        return instanceId.equals(id.instanceId) && transactionId == id.transactionId
                && transactionTime.equals(id.transactionTime);
    }

    @Override
    public String toString() {
        return transactionId + "@" + instanceId + "::" + transactionTime;
    }

}
