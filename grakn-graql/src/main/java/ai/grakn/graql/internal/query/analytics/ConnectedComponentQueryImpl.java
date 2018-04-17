/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.analytics.ConnectedComponentQuery;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class ConnectedComponentQueryImpl<T> extends AbstractClusterQuery<T, ConnectedComponentQuery<T>>
        implements ConnectedComponentQuery<T> {

    private boolean members = false;
    private boolean anySize = true;
    private Optional<ConceptId> sourceId = Optional.empty();
    private long clusterSize = -1L;

    ConnectedComponentQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    public final ComputeJob<T> createJob() {
        return queryRunner().run(this);
    }

    @Override
    public ConnectedComponentQuery<Map<String, Set<String>>> membersOn() {
        this.members = true;
        return (ConnectedComponentQuery<Map<String, Set<String>>>) this;
    }

    @Override
    public ConnectedComponentQuery<Map<String, Long>> membersOff() {
        this.members = false;
        return (ConnectedComponentQuery<Map<String, Long>>) this;
    }

    @Override
    public final boolean isMembersSet() {
        return members;
    }

    @Override
    public ConnectedComponentQuery<T> of(ConceptId conceptId) {
        this.sourceId = Optional.ofNullable(conceptId);
        return this;
    }

    @Override
    public final Optional<ConceptId> sourceId() {
        return sourceId;
    }

    @Override
    public ConnectedComponentQuery<T> clusterSize(long clusterSize) {
        this.anySize = false;
        this.clusterSize = clusterSize;
        return this;
    }

    @Override
    @Nullable
    public final Long clusterSize() {
        return anySize ? null : clusterSize;
    }

    @Override
    ClusterMeasure getMethod() {
        return ClusterMeasure.CONNECTED_COMPONENT;
    }

    @Override
    String graqlString() {
        final String[] string = {super.graqlString()};
        List<String> options = new ArrayList<>();
        if (sourceId.isPresent()) {
            options.add(" source = " + sourceId.get().getValue());
        }
        if (members) {
            options.add(" members = true");
        }
        if (!anySize) {
            options.add(" size = " + clusterSize);
        }
        if (!options.isEmpty()) {
            string[0] += " where";
            options.forEach(option -> string[0] += option);
        }
        string[0] += ";";

        return string[0];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ConnectedComponentQueryImpl<?> that = (ConnectedComponentQueryImpl<?>) o;

        return sourceId.equals(that.sourceId) && members == that.members &&
                anySize == that.anySize && clusterSize == that.clusterSize;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + sourceId.hashCode();
        result = 31 * result + (members ? 1 : 0);
        result = 31 * result + (anySize ? 1 : 0);
        result = 31 * result + (int) (clusterSize ^ (clusterSize >>> 32));
        return result;
    }
}
