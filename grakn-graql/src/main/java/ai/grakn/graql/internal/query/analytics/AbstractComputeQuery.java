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

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.internal.query.AbstractExecutableQuery;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

abstract class AbstractComputeQuery<T, V extends ComputeQuery<T>>
        extends AbstractExecutableQuery<T> implements ComputeQuery<T> {

    private Optional<GraknTx> tx;
    private GraknComputer graknComputer = null;
    private boolean includeAttribute;
    private ImmutableSet<Label> subLabels = ImmutableSet.of();

    private static final boolean DEFAULT_INCLUDE_ATTRIBUTE = false;

    AbstractComputeQuery(Optional<GraknTx> tx) {
        this(tx, DEFAULT_INCLUDE_ATTRIBUTE);
    }

    AbstractComputeQuery(Optional<GraknTx> tx, boolean includeAttribute) {
        this.tx = tx;
        this.includeAttribute = includeAttribute;
    }

    @Override
    public final Optional<GraknTx> tx() {
        return tx;
    }

    @Override
    public final V withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return (V) this;
    }

    @Override
    public final V in(String... subTypeLabels) {
        return in(Arrays.stream(subTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public V in(Collection<Label> subLabels) {
        this.subLabels = ImmutableSet.copyOf(subLabels);
        return (V) this;
    }

    @Override
    public final ImmutableSet<Label> subLabels() {
        return subLabels;
    }

    @Override
    public final V includeAttribute() {
        this.includeAttribute = true;
        return (V) this;
    }

    @Override
    public final boolean getIncludeAttribute() {
        return includeAttribute || isStatisticsQuery() || subTypesContainsImplicitOrAttributeTypes();
    }

    @Override
    public final void kill() {
        if (graknComputer != null) {
            graknComputer.killJobs();
        }
    }

    private boolean subTypesContainsImplicitOrAttributeTypes() {
        if (!tx.isPresent()) {
            return false;
        }

        GraknTx theTx = tx.get();

        return subLabels.stream().anyMatch(label -> {
            SchemaConcept type = theTx.getSchemaConcept(label);
            return (type != null && (type.isAttributeType() || type.isImplicit()));
        });
    }

    abstract String graqlString();

    final String subtypeString() {
        return subLabels.isEmpty() ? ";" : " in "
                + subLabels.stream().map(StringConverter::typeLabelToString).collect(joining(", ")) + ";";
    }

    @Override
    public final String toString() {
        return "compute " + graqlString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractComputeQuery<?, ?> that = (AbstractComputeQuery<?, ?>) o;

        return tx.equals(that.tx) && includeAttribute == that.includeAttribute && subLabels.equals(that.subLabels);
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttribute);
        result = 31 * result + subLabels.hashCode();
        return result;
    }
}
