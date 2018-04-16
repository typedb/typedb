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

import ai.grakn.API;
import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

abstract class AbstractCentralityQuery<V extends ComputeQuery<Map<Long, Set<String>>>>
        extends AbstractComputeQuery<Map<Long, Set<String>>, V> {

    private ImmutableSet<Label> ofLabels = ImmutableSet.of();

    private static final boolean INCLUDE_ATTRIBUTE = true; // TODO: REMOVE THIS LINE

    AbstractCentralityQuery(Optional<GraknTx> tx) {
        super(tx, INCLUDE_ATTRIBUTE);
    }

    /**
     * The centrality measures supported.
     */
    enum CentralityMeasure {

        DEGREE("degree"),
        K_CORE("k-core");
        private final String name;

        CentralityMeasure(String name) {
            this.name = name;
        }

        @CheckReturnValue
        public String getName() {
            return name;
        }
    }

    @API
    public final V of(String... ofTypeLabels) {
        return of(Arrays.stream(ofTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    public final V of(Collection<Label> ofLabels) {
        this.ofLabels = ImmutableSet.copyOf(ofLabels);
        return (V) this;
    }

    public final Set<Label> targetLabels() {
        return ofLabels;
    }

    abstract CentralityMeasure getMethod();

    @Override
    String graqlString() {
        String string = "centrality";
        if (!targetLabels().isEmpty()) {
            string += " of " + targetLabels().stream()
                    .map(StringConverter::typeLabelToString)
                    .collect(joining(", "));
        }
        string += subtypeString() + " using " + getMethod().getName();

        return string;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractCentralityQuery<?> that = (AbstractCentralityQuery<?>) o;

        return ofLabels.equals(that.ofLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ofLabels.hashCode();
        return result;
    }
}
