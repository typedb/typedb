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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query.analytics;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.graql.ComputeQuery;

import javax.annotation.CheckReturnValue;
import java.util.Optional;

abstract class AbstractClusterQuery<T, V extends ComputeQuery<T>>
        extends AbstractComputeQuery<T, V> {
    AbstractClusterQuery(Optional<GraknTx> tx) {
        super(tx);
    }

    /**
     * The centrality measures supported.
     */
    enum ClusterMeasure {

        CONNECTED_COMPONENT("connected-component"),
        K_CORE("k-core");
        private final String name;

        ClusterMeasure(String name) {
            this.name = name;
        }

        @CheckReturnValue
        public String getName() {
            return name;
        }
    }

    abstract ClusterMeasure getMethod();

    @Override
    String graqlString() {
        String string = "cluster";
        string += subtypeString() + " using " + getMethod().getName();

        return string;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractClusterQuery that = (AbstractClusterQuery) o;

        return getMethod() == that.getMethod();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getMethod().hashCode();
        return result;
    }
}
